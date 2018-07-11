package com.yao.extensions.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.support.NullValue;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheWriter;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author lirui
 * @date 2018-07-10
 * @description 重写spring cache默认的读写机制
 * 默认方法返回值是序列化成JOSN,放入redis中的（String类型value为JSON串）
 * 改造的目的： 当返回值的类型为Map<String,String>时在redis中保存为Hash类型
 * 流程：
 *  业务方法——序列化——>RedisCache———保存redis的操作类RedisCacheWriter———>redis
 */
@Component
public class HyBootRedisCacheWriter implements RedisCacheWriter {

    private RedisConnectionFactory connectionFactory;

    private final Duration sleepTime;

    /**
     * null值
     */
    private static final byte[] BINARY_NULL_VALUE =
            new JdkSerializationRedisSerializer().serialize(NullValue.INSTANCE);

    /**
     * redis操作客户端
     */
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * redis缓存全局配置
     */
    private RedisCacheConfiguration redisCacheConfiguration;

    /**
     * 定义序列化类
     */
    private final static GenericJackson2JsonRedisSerializer serializer =
            new GenericJackson2JsonRedisSerializer();

    /**
     * springCache默认缓存操作类
     */
    private RedisCacheWriter defaultRedisCacheWriter;

    public HyBootRedisCacheWriter(@Autowired RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.sleepTime = Duration.ZERO;
        defaultRedisCacheWriter = RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory);
    }

    /**
     * @return void
     * @Author: lirui
     * @Date: 2018-7-11
     * @params [name, key, value, ttl]
     * @Description: 写入redis缓存, 如果方法返回值类型为Map, 则把结果在redis中保存为Hash类型
     */
    @Override
    public void put(String name, byte[] key, byte[] value, Duration ttl) {

        //判断返回值是否为空,只有当返回值不为null时才进入Map判断
        if (!Arrays.equals(BINARY_NULL_VALUE, value)) {
            Object object = serializer.deserialize(value);
            //判断是否map类型
            if (Map.class.isAssignableFrom(object.getClass())) {
                Map map = (Map) object;

                //判断map类型是否符合Map<String,String>
                Set<Map.Entry> set = map.entrySet();

                boolean continueFlag = set.stream().allMatch(entry -> {
                    //只有当key,value都是String类型时才重写默认的保存操作
                    if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                        return true;
                    }
                    return false;
                });

                if (continueFlag) {
                    String cacheKey = redisCacheConfiguration.getKeySerializationPair().getReader().read(ByteBuffer.wrap(key));
                    //把map写入缓存并设置失效时间
                    stringRedisTemplate.opsForHash().putAll(cacheKey, map);
                    stringRedisTemplate.expire(cacheKey, ttl.getSeconds(), TimeUnit.SECONDS);
                    return;
                }
            }

        }

        defaultRedisCacheWriter.put(name, key, value, ttl);
    }

    /**
     * @return byte[]
     * @Author: lirui
     * @Date: 2018-7-11
     * @params [name, key]
     * @Description: 读缓存, 判断对应缓存在redis中的存储类型, 如果是Hash则自定义序列化
     */
    @Override
    public byte[] get(String name, byte[] key) {

        final DataType execute = execute(name, connection -> connection.type(key));
        //判断在redis是否map类型
        if (DataType.HASH == execute) {
            String cacheKey = redisCacheConfiguration.getKeySerializationPair().getReader().read(ByteBuffer.wrap(key));
            //从redis中取出对应的值
            Map map = stringRedisTemplate.opsForHash().entries(cacheKey);

            return serializer.serialize(map);

        }
        return defaultRedisCacheWriter.get(name, key);
    }

    @Override
    public byte[] putIfAbsent(String name, byte[] key, byte[] value, Duration ttl) {
        byte[] bytes = defaultRedisCacheWriter.putIfAbsent(name, key, value, ttl);
        return bytes;
    }

    @Override
    public void remove(String name, byte[] key) {
        defaultRedisCacheWriter.remove(name, key);

    }

    @Override
    public void clean(String name, byte[] pattern) {
        defaultRedisCacheWriter.clean(name, pattern);
    }

    /**
     * Explicitly set a write lock on a cache.
     *
     * @param name the name of the cache to lock.
     */
    void lock(String name) {
        execute(name, connection -> doLock(name, connection));
    }

    /**
     * Explicitly remove a write lock from a cache.
     *
     * @param name the name of the cache to unlock.
     */
    void unlock(String name) {
        executeLockFree(connection -> doUnlock(name, connection));
    }

    private Boolean doLock(String name, RedisConnection connection) {
        return connection.setNX(createCacheLockKey(name), new byte[0]);
    }

    private Long doUnlock(String name, RedisConnection connection) {
        return connection.del(createCacheLockKey(name));
    }

    boolean doCheckLock(String name, RedisConnection connection) {
        return connection.exists(createCacheLockKey(name));
    }

    /**
     * @return {@literal true} if {@link RedisCacheWriter} uses locks.
     */
    private boolean isLockingCacheWriter() {
        return !sleepTime.isZero() && !sleepTime.isNegative();
    }

    private <T> T execute(String name, Function<RedisConnection, T> callback) {

        RedisConnection connection = connectionFactory.getConnection();
        try {

            checkAndPotentiallyWaitUntilUnlocked(name, connection);
            return callback.apply(connection);
        } finally {
            connection.close();
        }
    }

    private void executeLockFree(Consumer<RedisConnection> callback) {

        RedisConnection connection = connectionFactory.getConnection();

        try {
            callback.accept(connection);
        } finally {
            connection.close();
        }
    }

    private void checkAndPotentiallyWaitUntilUnlocked(String name, RedisConnection connection) {

        if (!isLockingCacheWriter()) {
            return;
        }

        try {

            while (doCheckLock(name, connection)) {
                Thread.sleep(sleepTime.toMillis());
            }
        } catch (InterruptedException ex) {

            // Re-interrupt current thread, to allow other participants to react.
            Thread.currentThread().interrupt();

            throw new PessimisticLockingFailureException(String.format("Interrupted while waiting to unlock cache %s", name),
                    ex);
        }
    }

    private static boolean shouldExpireWithin(@Nullable Duration ttl) {
        return ttl != null && !ttl.isZero() && !ttl.isNegative();
    }

    private static byte[] createCacheLockKey(String name) {
        return (name + "~lock").getBytes(StandardCharsets.UTF_8);
    }

    public RedisCacheConfiguration getRedisCacheConfiguration() {
        return redisCacheConfiguration;
    }

    public void setRedisCacheConfiguration(RedisCacheConfiguration redisCacheConfiguration) {
        this.redisCacheConfiguration = redisCacheConfiguration;
    }

}

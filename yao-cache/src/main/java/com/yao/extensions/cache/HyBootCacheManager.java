package com.yao.extensions.cache;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lirui
 * @date 2018-07-10
 * @description hyboot缓存管理类
 */
@Configuration
@ConfigurationProperties(prefix = "spring.cache")
public class HyBootCacheManager implements ApplicationContextAware {

    /**
     * spring上下文
     */
    private ApplicationContext applicationContext;

    /**
     * 缓存过期时间（扩展springCache）
     */
    private Map<String,Long> ttl = new HashMap<>();


    /**
     * @return org.springframework.cache.CacheManager
     * @Author: lirui
     * @Date: 2018-7-6
     * @params [redisConnectionFactory]
     * @Description: 获取redis缓存管理类, 增加缓存的过期时间配置
     */
    @Bean
    public CacheManager cacheManager(@Autowired RedisConnectionFactory redisConnectionFactory,@Autowired HyBootRedisCacheWriter redisCacheWriter) {

        //设置过期时间
        Map<String, RedisCacheConfiguration> cacheConfigurations = new HashMap<>();

        //默认配置,
        RedisCacheConfiguration redisCacheConfiguration = RedisCacheConfiguration.defaultCacheConfig()
                //自定义序列化
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(
                        new GenericJackson2JsonRedisSerializer()))
                //定义key的前缀
                .computePrefixWith((name) -> {
                    return applicationContext.getId() + ":" + name + ":";
                });

        //循环并添加到缓存管理中
        ttl.forEach((key, value) -> {
            cacheConfigurations.put(key, redisCacheConfiguration.entryTtl(Duration.ofSeconds(value)));
        });

        //设置HyBootRedisCacheWriter的redisCacheConfiguration值
        redisCacheWriter.setRedisCacheConfiguration(redisCacheConfiguration);

        //创建cacheManager
        RedisCacheManager cacheManager = RedisCacheManager.RedisCacheManagerBuilder
                .fromCacheWriter(redisCacheWriter)
                .cacheDefaults(redisCacheConfiguration)
                .withInitialCacheConfigurations(cacheConfigurations).build();

        return cacheManager;
    }

    /**
     * @return com.chyjr.hyboot.autoconfigure.extensions.cache.HyBootCacheKeyGenerator
     * @Author: lirui
     * @Date: 2018-7-9
     * @params []
     * @Description: 自定义缓存生成器
     */
    @Bean(name = "keyGenerator")
    public HyBootCacheKeyGenerator keyGenerator() {
        return new HyBootCacheKeyGenerator();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Map<String, Long> getTtl() {
        return ttl;
    }

    public void setTtl(Map<String, Long> ttl) {
        this.ttl = ttl;
    }
}

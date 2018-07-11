package com.yao.extensions.cache;

import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleKey;

import java.lang.reflect.Method;

/**
 * @author lirui
 * @date 2018-07-09
 * @description 缓存key生成器
 * <p>
 * 不指定缓存key时会采用默认的key生成器,由于默认的key生产器不包含方法名称,即缓存很容易被覆盖,
 * 所以要求两种方式配置（两种方式至少指定一种,当两种都指定时,则第二种失效）：
 * 1、使用缓存注解时可以自己定义key的值（注意自己定义的key一定不能重复,否则会被覆盖,key定义支持SpEL）,
 * 2、可以用框架定义的key生成器
 * <p>
 */
public class HyBootCacheKeyGenerator implements KeyGenerator {

    /**
     * @return java.lang.Object
     * @Author: lirui
     * @Date: 2018-7-9
     * @params [target, method, params]
     * @Description: 自定义key的生产规则
     */
    @Override
    public Object generate(Object target, Method method, Object... params) {
        StringBuffer key = new StringBuffer();
        //缓存key第一部分：类名
        String className = target.getClass().getName();
        key.append(className + ":");
        //换成第二部分：方法名称
        String methodName = method.getName();
        key.append(methodName + ":");
        //换成第三部分：参数采用自带的SimpleKey
        String simpleKey = new SimpleKey(params).toString();
        key.append(simpleKey);
        return key;
    }
}

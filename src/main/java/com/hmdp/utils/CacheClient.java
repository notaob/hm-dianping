package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value,Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value,Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback
            , Long time, TimeUnit unit){
        //key
        String key = keyPrefix  + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断redis存在
        if(StrUtil.isNotBlank(json)){
            //redis存在返回
            return JSONUtil.toBean(json, type);
        }
        //判断redis是否命中的是否是空值
        if(json != null){
            //返回错误
            return null;
        }
        //redis不存在，id查数据库

        R r = dbFallback.apply(id);
        //数据库不存在，返回错误
        if (r==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",time, unit);
            //返回错误
            return null;
        }
        //数据库存在，写入redis
        set(key, r, time, unit);
        //返回
        return r;
    }

    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, String locKeyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback
            , Long time, TimeUnit unit){
        //key
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断redis存在
        if(StrUtil.isBlank(json)){
            //redis不存在返回
            return null;
        }
        //命中，把json转为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r=JSONUtil.toBean((JSONObject)redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期
            return r;
        }
        //已过期，缓存重建
        //获取互斥锁
        String lockKey = locKeyPrefix + id;
        boolean isLock = tryLock(lockKey);
        //获取锁成功，开启独立线程，实现缓存重建
        if (isLock){
            //doublecheck
            //命中，把json转为对象
            redisData = JSONUtil.toBean(json, RedisData.class);
            r=JSONUtil.toBean((JSONObject)redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            //判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())){
                //未过期
                return r;
            }
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建缓存
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //返回过期商铺信息
        return r;
    }
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}

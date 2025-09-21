package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static java.lang.Thread.sleep;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
//        缓存穿透
        Shop shop=cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class
                ,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
//        逻辑过期解决缓存击穿
//        Shop shop=cacheClient
//                .queryWithLogicalExpire(CACHE_SHOP_KEY,LOCK_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if (shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    private Shop queryWithMutex(Long id){
        //key
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断redis存在
        if(StrUtil.isNotBlank(shopJson)){
            //redis存在返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断redis是否命中的是否是空值
        if(shopJson != null){
            //返回错误
            return null;
        }
        //4 实现缓存重建
        //4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否成功
            if (!isLock){
                //4.3获取锁失败，休眠并重试
                sleep(50);
                return queryWithMutex(id);
            }
            //doublecheck
            //从redis查询商铺缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //判断redis存在
            if(StrUtil.isNotBlank(shopJson)){
                //redis存在返回
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            //判断redis是否命中的是否是空值
            if(shopJson != null){
                //返回错误
                return null;
            }
            //4.4成功，redis不存在，id查数据库
            shop = getById(id);
            sleep(200);
            //数据库不存在，返回错误
            if (shop==null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误
                return null;
            }
            //数据库存在，写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //释放互斥锁
        unLock(lockKey);
        //返回
        return shop;
    }
    private Shop queryWithPassThrough(Long id){
        //key
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断redis存在
        if(StrUtil.isNotBlank(shopJson)){
            //redis存在返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断redis是否命中的是否是空值
        if(shopJson != null){
            //返回错误
            return null;
        }
        //redis不存在，id查数据库
        Shop shop = getById(id);
        //数据库不存在，返回错误
        if (shop==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误
            return null;
        }
        //数据库存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        // 1. 更新数据库
        boolean success = updateById(shop);

        if (success) {
            // 2. 删除缓存（缓存失效策略）
            stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
            return Result.ok();
        }

        return Result.fail("更新失败");
    }
}

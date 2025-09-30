package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
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

    @Resource
    private IShopService shopService;
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

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //是否需要根据坐标查询
        if (x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = shopService.query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //查询redis 按照距离排序分页 结果: shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .radius(key,
                        new Circle(new Point(x, y), new Distance(5000)),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end)
                );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        //解析出id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        //截取from ~ end
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        //根据id查询shop
        String idStr = StrUtil.join(",",ids);
        List<Shop> shops = shopService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + idStr +")")
                .list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        //返回
        return Result.ok(shops);
    }
}

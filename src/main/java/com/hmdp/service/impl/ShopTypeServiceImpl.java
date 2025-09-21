package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        //key
        String key = CACHE_SHOPTYPE_KEY;
        //1.从redis查询商铺缓存
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);
        //判断redis存在
        if(StrUtil.isNotBlank(shopTypeJson)){
            //返回数据
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //redis不存在，查数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //数据库不存在，返回错误
        if (shopTypeList.isEmpty()){
            return Result.fail("店铺种类不存在");
        }
        //数据库存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shopTypeList));

        //返回
        return Result.ok(shopTypeList);
    }
}

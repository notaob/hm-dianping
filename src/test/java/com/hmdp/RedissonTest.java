package com.hmdp;

import lombok.extern.slf4j.Slf4j;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class RedissonTest {
    @Resource
    private RedissonClient redissonClient1;
    @Resource
    private RedissonClient redissonClient2;
    @Resource
    private RedissonClient redissonClient3;

    private RLock lock;

    @Before
    public void setUp() {
        RLock lock1=redissonClient1.getLock("lock");
        RLock lock2=redissonClient2.getLock("lock");
        RLock lock3=redissonClient3.getLock("lock");
        lock=redissonClient1.getMultiLock(lock1,lock2,lock3);
    }
    
    @Test
    public void test() throws InterruptedException {
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        log.info("获取锁结果: {}", isLock);
        if (isLock) {
            lock.unlock();
        }
    }

}
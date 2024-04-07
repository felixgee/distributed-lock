package com.fitcle.locks.test.redis;

import com.fitcle.locks.redis.NettyRedisClient;
import com.fitcle.locks.redis.RedisConnection;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public abstract class NettySetup {

    static NettyRedisClient redisClient;
    static RedisConnection connection;

    @BeforeSuite
    public static void tearUp() {
        redisClient = NettyRedisClient.create("192.168.146.130", 6379);
        connection = redisClient.connect(true);
    }

    @AfterSuite
    public static void tearDown() {
        redisClient.close();
    }
}

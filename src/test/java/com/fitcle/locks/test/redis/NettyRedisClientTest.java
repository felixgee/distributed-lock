package com.fitcle.locks.test.redis;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class NettyRedisClientTest extends NettySetup {

    @Test
    public void testSubscribe() {
        Object res = connection.subscribe("lock");
        Assert.assertEquals("1", res);
    }

    @Test
    public void testUnsubscribe() {
        Object res = connection.unsubscribe("lock");
        Assert.assertEquals("0", res);
    }

    @Test
    public void testEval() {
        String script = """
                redis.call('set', KEYS[1], ARGV[1], 'px', 6000)
                return redis.call('get', KEYS[1])
                """;
        String value = UUID.randomUUID().toString();
        Object res = connection.eval(script, List.of("test-eval"), List.of(value));
        Assert.assertEquals(value, res);
    }

    @Test
    public void testMultipleThread() {
        List<Object> thread1 = new ArrayList<>();
        List<Object> thread2 = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            int count = 10000;
            while (count-- > 0) {
                thread1.add(connection.set("test", "haha", 60000));
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            int count = 10000;
            while (count-- > 0) {
                thread2.add(connection.setnx("test", "haha", 60000));
            }
            latch.countDown();
        }).start();

        Assert.assertFalse(thread1.stream().anyMatch(item -> !"1".equals(item)));
        Assert.assertFalse(thread2.stream().anyMatch(Objects::nonNull));
    }

}

package com.fitcle.locks.test.redis;

import com.fitcle.locks.DistributedLock;
import com.fitcle.locks.DistributedLockException;
import com.fitcle.locks.redis.RedisDistributedLock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisDistributedLockTest extends NettySetup {

    private final String lockName = "distributed-lock";
    private final String listenChannel = "unlock";

    private DistributedLock lock;

    @AfterMethod
    public void cleanUp() {
        if (Objects.nonNull(lock)) {
            lock.unlock();
        }
        connection.unsubscribe(listenChannel);
        connection.del(lockName);
    }

    @Test
    public void testSingleThreadTryLock() {
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        Assert.assertTrue(lock.tryLock());
        Assert.assertNotNull(connection.get(lockName));
    }

    @Test
    public void testSingleThreadTryLockTtl() {
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        Assert.assertTrue(lock.tryLock(1000, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(connection.get(lockName));
    }

    @Test
    public void testSingleThreadTryLockTtlTimeout() {
        connection.set(lockName, "aa", 60000);
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        Assert.assertFalse(lock.tryLock(1000, TimeUnit.MILLISECONDS));
        lock = null;
    }

    @Test
    public void testSingleThreadLock() {
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        lock.lock();
        Assert.assertNotNull(connection.get(lockName));
    }

    @Test
    public void testSingleThreadLockTtl() {
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        lock.lock(1000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(connection.get(lockName));
    }

    @Test
    public void testSingleThreadLockTtlTimeout() {
        connection.set(lockName, "aa", 60000);
        Assert.assertThrows(DistributedLockException.class, () -> {
            try {
                lock = new RedisDistributedLock(connection, lockName, listenChannel);
                lock.lock(1000, TimeUnit.MILLISECONDS);
            } finally {
                lock = null;
            }
        });
    }

    @Test
    public void testSingleThreadUnlock() {
        lock = new RedisDistributedLock(connection, lockName, listenChannel);
        Assert.assertTrue(lock.tryLock());
        lock.unlock();
        Assert.assertNull(connection.get(lockName));
        lock = null;
    }

    @Test
    public void testMultiThreadTryLock() throws InterruptedException {
        List<Boolean> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            boolean hasLock = false;
            RedisDistributedLock currentLock = null;
            try {
                currentLock = new RedisDistributedLock(connection, lockName, listenChannel);
                hasLock = currentLock.tryLock();
                result.add(hasLock);
            } finally {
                if (hasLock) {
                    currentLock.unlock();
                }
                lock = null;
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            boolean hasLock = false;
            RedisDistributedLock currentLock = null;
            try {
                currentLock = new RedisDistributedLock(connection, lockName, listenChannel);
                hasLock = currentLock.tryLock();
                result.add(hasLock);
            } finally {
                if (hasLock) {
                    currentLock.unlock();
                }
                lock = null;
                latch.countDown();
            }
        }).start();

        latch.await();

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().anyMatch(Boolean.TRUE::equals));
        Assert.assertTrue(result.stream().anyMatch(Boolean.FALSE::equals));
    }

    @Test
    public void testCrossJvmMultiThreadTryLockTtl() {
        DistributedLock lock = new RedisDistributedLock(connection, lockName, listenChannel);
        int loopCount = 180;
        List<Boolean> result = new ArrayList<>();
        while (loopCount-- > 0) {
            if (lock.tryLock(1000, TimeUnit.MILLISECONDS)) {
                result.add(true);
                lock.unlock();
            } else {
                result.add(false);
            }
        }
        Assert.assertTrue(result.stream().anyMatch(Boolean.TRUE::equals));
    }
}

/*
 * Copyright 2024 Felix Luo.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fitcle.locks.redis;

import com.fitcle.locks.DistributedLock;
import com.fitcle.locks.DistributedLockException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The implementation of DistributedLock based on redis.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
@Slf4j
public class RedisDistributedLock implements DistributedLock {
    /**
     * Default lock ttl, in our view, 30 seconds is long enough for most business logic.
     * And if the lock is not released normally, it's tolerant that the lock will be
     * expired in redis after 30 seconds.
     */
    private static final long DEFAULT_LOCK_TTL_MS = 30000;

    /**
     * It's a constant that redis returns when a set command is successful.
     */
    private static final String LOCK_SUCCESS_MSG = "OK";

    /**
     * It's a constant that redis returns when a del command is successful.
     */
    private static final String UNLOCK_SUCCESS_FLAG = "1";

    /**
     * The lua script to release a lock
     */
    private static final String LUA_UNLOCK_SCRIPT = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                if redis.call('del', KEYS[1]) == 1 then
                    redis.call('publish', KEYS[2], ARGV[1])
                    return 1
                end
            end
            return 0
            """;


    /**
     * The redis connection
     */
    private final RedisConnection connection;
    /**
     * the distributed lock name
     */
    private final String lock;
    /**
     * it's a UUID to indicate which thread is acquiring the lock
     */
    private final String uuid;
    /**
     * lock timeout in milliseconds
     */
    private final long lockTimeout;
    /**
     * the redis message channel name to be subscribed
     */
    private final String listenChannel;
    /**
     * the lock state
     */
    private final LockState state;

    /**
     * constructor with default ttl 30000 milliseconds.
     *
     * @param connection the redis conection
     * @param lock the lock name
     * @param listenChannel the redis channel
     */
    public RedisDistributedLock(RedisConnection connection, String lock, String listenChannel) {
        this(connection, lock, listenChannel, DEFAULT_LOCK_TTL_MS);
    }

    /**
     * constructor with mandatory parameters.
     *
     * @param connection the redis connection
     * @param lock the lock name
     * @param listenChannel the redis channel name
     * @param ttl lock expires in milliseconds
     */
    public RedisDistributedLock(RedisConnection connection, String lock, String listenChannel, long ttl) {
        this.connection = connection;
        this.lock = lock;
        this.uuid = UUID.randomUUID().toString().replace("-", "");
        this.lockTimeout = ttl;
        this.listenChannel = listenChannel;
        this.state = LockStateContainer.register(lock, listenChannel);
    }

    /**
     * @see com.fitcle.locks.DistributedLock
     */
    @Override
    public void lock() {
        if (heldRemoteLock()) {
            // re-enter the lock, increase the lock counter
            state.getCounter().incrementAndGet();
            return;
        }

        while (!tryLock()) {
            /* if current thread can not get the lock, then it will come into a waiters queue,
             * and subscribe the unlock event. Once the unlock event is received, the waiter
             * in the head of the waiters queue will be notified.
             */
            state.getLocalWaiters().offer(Thread.currentThread());
            connection.subscribe(this.listenChannel);
            LockSupport.park();
        }
    }

    /**
     * @see com.fitcle.locks.DistributedLock
     */
    @Override
    public void lock(long timeout, TimeUnit unit) {
        if (Objects.isNull(unit)) {
            throw new NullPointerException("TimeUnit parameter should not be null");
        }

        if (heldRemoteLock()) {
            state.getCounter().incrementAndGet();
            return;
        }

        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        long ttl;

        while (!tryLock()) {
            ttl = deadline - System.currentTimeMillis();
            if (timeout > 0 && ttl < 0) {
                if (state.getLocalLock().isHeldByCurrentThread()) {
                    // ensure the local lock is released
                    state.getLocalLock().unlock();
                }
                throw new DistributedLockException("Failed to get lock during time duration: " + timeout);
            }

            state.getLocalWaiters().offer(Thread.currentThread());
            connection.subscribe(this.listenChannel);

            if (timeout < 0) {
                LockSupport.park();
            } else if ((ttl = deadline - System.currentTimeMillis()) > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(ttl));
            }
        }
    }

    /**
     * @see com.fitcle.locks.DistributedLock
     */
    @Override
    public boolean tryLock() {
        if (heldRemoteLock()) {
            return state.getCounter().incrementAndGet() > 0;
        }

        // fast fail if current thread cannot get the local lock
        if (!(state.getLocalLock().isHeldByCurrentThread() || state.getLocalLock().tryLock())) {
            return false;
        }

        return this.tryAcquire();
    }

    /**
     * @see com.fitcle.locks.DistributedLock
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        long start = System.currentTimeMillis();

        if (Objects.isNull(unit)) {
            throw new NullPointerException("TimeUnit should not be null");
        }

        if (heldRemoteLock()) {
            return state.getCounter().incrementAndGet() > 0;
        }

        long ttl = unit == TimeUnit.MILLISECONDS ? timeout : unit.toMillis(timeout);

        // fast fail if current thread cannot get the local lock
        boolean heldLocalLock = state.getLocalLock().isHeldByCurrentThread() || state.getLocalLock().tryLock();
        if (!heldLocalLock) {
            return false;
        }

        while (!tryAcquire()) {
            if (ttl <= 0) {
                return false;
            }
            if (start + ttl - System.currentTimeMillis() < 0) {
                return false;
            }
            state.getLocalWaiters().offer(Thread.currentThread());
            connection.subscribe(this.listenChannel);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(start + ttl - System.currentTimeMillis()));
        }

        return true;
    }

    /**
     * @see com.fitcle.locks.DistributedLock
     */
    @Override
    public void unlock() {
        if (!heldRemoteLock()) { // protect the lock
            throw new DistributedLockException("The lock is not held by current thread");
        }

        /*
         * If the lock counter greater than 0 after decrease, means the lock is still held by current thread.
         * Only the lock counter is ZERO, the distributed lock will be completely released.
         */
        if (state.getCounter().decrementAndGet() <= 0) {
            // stop the scheduler that extends the ttl of current lock
            RedisLockStateWatcher.INSTANCE.unwatch(this.lock);
            try {
                 Object result = connection.eval(LUA_UNLOCK_SCRIPT, List.of(this.lock, this.listenChannel), List.of(this.uuid));
                if (UNLOCK_SUCCESS_FLAG.equals(result)) {
                    log.debug("Successfully unlock");
                } else {
                    log.error("Failed to unlock: {}", result);
                }
            } catch (Exception e) {
                log.warn("Failed to release the lock, cause: {}", e.getMessage());
            } finally {
                state.setOwner(null);
                while (state.getLocalLock().isHeldByCurrentThread()) {
                    state.getLocalLock().unlock();
                }
            }
        }
    }

    private boolean tryAcquire() {
        try {
            Object result = connection.setnx(this.lock, this.uuid, this.lockTimeout);
            if (LOCK_SUCCESS_MSG.equals(result)) {
                state.setOwner(Thread.currentThread());
                state.getCounter().incrementAndGet();
                RedisLockStateWatcher.INSTANCE.watch(connection, this.lock, this.uuid, this.lockTimeout);
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to acquire the lock", e);
        }

        // if a thread can not get the remote lock, the local lock it holds must be released, so that
        // other local thread can compete the local lock.
        if (state.getLocalLock().isHeldByCurrentThread()) {
            state.getLocalLock().unlock();
        }

        return false;
    }

    private boolean heldRemoteLock() {
        return Objects.nonNull(state)
                && state.getLocalLock().isHeldByCurrentThread()
                && Thread.currentThread().equals(state.getOwner())
                && state.getCounter().get() > 0;
    }
}

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
package com.fitcle.locks;

import java.util.concurrent.TimeUnit;

/**
 * This interface is designed as a JVM-cross lock. Its implementations should consider the JVM-cross situation
 * that the distributed locks are used in.
 * <br/>
 * <br/>
 * When we must use a distributed lock to access the shared resource, this interface can provide help and its
 * usage is very concise. Below code snippet is an example:
 *
 * <pre>
 * {@code
 *      DistributedLock lock = ...;
 *      lock.lock();
 *      try {
 *          // do something with the shared resource
 *      } finally {
 *          lock.unlock();
 *      }
 * }
 * </pre>
 *
 * @see com.fitcle.locks.redis.RedisDistributedLock
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
public interface DistributedLock {

    /**
     * Acquire the lock. It will block current thread and wait for the lock until the distributed lock
     * is successfully acquired.
     */
    void lock();

    /**
     * Acquire the lock in given time period. It will block current thread and wait for the lock until
     * the distributed lock is successfully acquired or the time is elapsed.
     * <br>
     * <br>
     * If the lock is not acquired successfully in given time period, it will throw a DistributedLockException.
     * <br>
     * <br>
     * If the time unit parameter is null, it will throw a NullPointerException.
     * <br>
     * <br>
     * @param timeout the max time that this call will wait for
     * @param unit the time unit, should not be null
     * @see com.fitcle.locks.DistributedLockException
     */
    void lock(long timeout, TimeUnit unit);

    /**
     * Acquire the lock.
     * @return true if the lock is successfully acquired, otherwise will return false immediately
     */
    boolean tryLock();

    /**
     * Acquire the lock in given time period.
     * If the time unit parameter is null, it will throw a NullPointerException.
     *
     * @param timeout the max time that this call will wait for
     * @param unit the time unit, should not be null
     * @return true if the lock is successfully acquired, otherwise will wait for the lock
     *         until the lock is successfully acquired or the given time is elapsed
     */
    boolean tryLock(long timeout, TimeUnit unit);

    /**
     * Release the lock. If the lock is not held by current thread, it will throw a DistributedLockException.
     * @see com.fitcle.locks.DistributedLockException
     */
    void unlock();
}
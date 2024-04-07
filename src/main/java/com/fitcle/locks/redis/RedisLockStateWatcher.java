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

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.*;

/**
 * Scheduler to extend the lock ttl.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
@Slf4j
enum RedisLockStateWatcher {

    /**
     * Singleton instance holder
     */
    INSTANCE;

    /**
     * distributed lock watchers' container.
     */
    private final ConcurrentMap<String, ScheduledFuture<?>> watchTasks = new ConcurrentHashMap<>();

    /**
     * Starts the scheduler to extend the ttl of distributed lock
     *
     * @param connection the redis connection
     * @param lock the lock name
     * @param uuid the UUID of lock instance
     * @param ttl the lock ttl, milliseconds
     */
    public void watch(final RedisConnection connection, final String lock, final String uuid, final long ttl) {
        long delay = ttl / 3; // when the time elapses 1/3 of the ttl, the scheduler starts extending the ttl
        ScheduledFuture<?> task = scheduler.scheduleWithFixedDelay(() -> {
            LockState state = LockStateContainer.getLockState(lock);
            if (Objects.isNull(state) || Objects.isNull(state.getOwner()) || state.getCounter().get() <= 0) {
                unwatch(lock);
                return;
            }

            try {
                connection.set(lock, uuid, ttl);
            } catch (Exception e) {
                log.error("Error occurred while trying to extend lock ttl", e);
                unwatch(lock);
            }
        }, ttl / 4, delay, TimeUnit.MILLISECONDS);

        watchTasks.put(lock, task);
    }

    /**
     * Stop the scheduler
     *
     * @param lock the lock name
     */
    public void unwatch(String lock) {
        ScheduledFuture<?> task = watchTasks.get(lock);
        if (Objects.nonNull(task) && !task.isCancelled()) {
            task.cancel(true);
            watchTasks.remove(lock);
        }
    }

    /**
     * Only needs 1 thread to execute the watchers' tasks
     */
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r);
        t.setName("redis-lock-watcher");
        t.setDaemon(true);
        t.setPriority(Thread.MAX_PRIORITY);
        return t;
    });
}

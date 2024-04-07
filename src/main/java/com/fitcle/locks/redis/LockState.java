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

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used to record the distributed lock information.
 * When multiple threads compete a same lock, the lock information is shared cross those threads.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
@Getter
final class LockState {
    /**
     * This attribute is used to control the competition of multiple local threads.
     * Only the winner of local threads has the chance to acquire remote lock.
     * This mechanism can reduce the competition of remote lock and improve the performance.
     */
    private final ReentrantLock localLock = new ReentrantLock();

    /**
     * to record the local lock waiters.
     */
    private final LinkedBlockingQueue<Thread> localWaiters = new LinkedBlockingQueue<>();

    /**
     * the lock counter.
     * If a thread holds the lock, this counter is greater than ZERO.
     * The lock can be re-entered, once the lock is re-entered, this counter will plus ONE.
     * A reentrant lock must call unlock method to re-balanced the lock counter.
     */
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * the distributed lock name.
     * If the instances of distributed lock have the same lock name, they are the same distributed lock.
     */
    private final String lockName;

    /**
     * the redis channel name to be subscribed by the lock waiters.
     */
    private final String listenChannel;

    /**
     * the remote lock owner
     */
    @Setter
    private volatile Thread owner;

    /**
     * This class is designed to be used in current package only.
     *
     * @param lock the distributed lock name
     * @param listenChannel the redis message channel name to be subscribed
     */
    LockState(String lock, String listenChannel) {
        this.lockName = lock;
        this.listenChannel = listenChannel;
    }
}

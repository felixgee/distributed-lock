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

import com.fitcle.locks.DistributedLockException;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The lock state container to share lock state cross different threads.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
final class LockStateContainer {
    /**
     * The lock state container.
     */
    private static final ConcurrentMap<String, LockState> container = new ConcurrentHashMap<>();

    /**
     * This lock state container holds the redis message channel name as the key, so that we can
     * get the corresponding lock state with the redis message channel name.
     */
    private static final ConcurrentMap<String, LockState> channelMap = new ConcurrentHashMap<>();

    /**
     * This method is called when a RedisDistributedLock instance is created.
     *
     * @param lock the distributed lock name
     * @param listenChannel the redis message channel name to be subscribed
     * @return the lock state instance
     */
    static LockState register(String lock, String listenChannel) {
        if (!container.containsKey(lock)) {
            LockState state = new LockState(lock, listenChannel);
            container.put(lock, state);
            channelMap.put(listenChannel, state);
        }
        return container.get(lock);
    }

    /**
     * Get the lock state instance by lock name.
     *
     * @param lock the distributed lock name
     * @return the lock state instance
     */
    static LockState getLockState(String lock) {
        LockState state = container.get(lock);
        if (Objects.isNull(state)) {
            throw new DistributedLockException(String.format("LockState of lock name %s has not been registered", lock));
        }
        return state;
    }

    /**
     * Get lock state by redis message channel name.
     *
     * @param listenChannel redis message channel name
     * @return the lock state instance
     */
    static LockState getLockStateByChannel(String listenChannel) {
        return channelMap.get(listenChannel);
    }

    /**
     * simply prevent this class to be instantiated
     */
    private LockStateContainer() {}
}

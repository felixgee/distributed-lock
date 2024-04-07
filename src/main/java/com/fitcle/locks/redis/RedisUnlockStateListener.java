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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * Listen to the unlock event from redis pub/sub channel.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
@Slf4j
class RedisUnlockStateListener implements NettyRedisMessageListener {

    @Override
    public void handleMessage(String channel, String message) {
        log.debug("Receives redis message from channel '{}': {}", channel, message);

        LockState state = LockStateContainer.getLockStateByChannel(channel);
        if (Objects.isNull(state)) {
            return;
        }
        LinkedBlockingQueue<Thread> waiters = state.getLocalWaiters();

        // ensure the thread is picked from the head of queue and the thread is still alive
        Thread t;
        for (t = waiters.poll(); t != null && !t.isAlive(); ) {
            t = waiters.poll();
        }

        // notify the waiter thread
        if (Objects.nonNull(t)) {
            LockSupport.unpark(t);
        }
    }
}

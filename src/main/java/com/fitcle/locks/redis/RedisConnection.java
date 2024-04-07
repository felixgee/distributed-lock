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

import java.util.List;

/**
 * This interface defines the methods of operating the redis.
 * <br>
 * <br>
 * Since our implementation of the distributed lock is based on redis, and only some redis operations
 * are used in our implementation, therefore, there are only several methods in this interface.
 * <br>
 * <br>
 * The reason why we don't use the existing redis client libs like jedis, lettuce and redisson is that
 * we would like to use a light weight redis client, which must exactly satisfy our requirement.
 * <br>
 * <br>
 * Jedis lib has the limitation of subscription operation to our implementation. The subscription operation
 * is blocking call, but the blocking call in our lock implementation is not acceptable.
 * <br>
 * <br>
 * Lettuce is too complicated to use while redisson is too heavy to us. Besides, redisson has already implemented
 * the distributed lock. It's duplicated function in our project.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
public interface RedisConnection {

    /**
     * Get a value from redis server by given key, if the given key does not exist in redis, it will return null.
     *
     * @param key the given key, should not be null
     * @return the corresponding value from redis
     */
    String get(String key);

    /**
     * Set a key-value pair into redis with milliseconds ttl.
     *
     * @param key the given key
     * @param value the given value
     * @param ttl timeout in milliseconds
     * @return OK if the operation is successful
     */
    Object set(String key, String value, long ttl);

    /**
     * Delete a key in redis
     * @param key the key to be deleted
     * @return if the key is successfully deleted, it will return 1, otherwise return 0
     */
    Object del(String key);

    /**
     * Set a key into redis only the key does not exist in redis.
     *
     * @param key the given key
     * @param value the given value
     * @param ttl timeout in milliseconds
     * @return if success, return 1; otherwise return null
     */
    Object setnx(String key, String value, long ttl);

    /**
     * Send a lua script to redis.
     *
     * @param script the lua script
     * @param keys the keys used in lua script
     * @param args the additional parameters used in lua script
     * @return depends on what is returned in lua script
     */
    Object eval(String script, List<String> keys, List<String> args);

    /**
     * Subscribe a redis message channel
     *
     * @param channel the redis message channel name
     * @return if success, return 1
     */
    Object subscribe(String channel);

    /**
     * Unsubscribe a redis message channel
     *
     * @param channel the redis message channel name
     * @return 0 if success
     */
    Object unsubscribe(String channel);
}

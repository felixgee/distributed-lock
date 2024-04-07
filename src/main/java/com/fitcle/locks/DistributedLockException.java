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

/**
 * This kink of exception is runtime exception that will be thrown during distributed lock operations.
 *
 * @since 17
 * @version 1.0
 * @author felix.luo.hwong@gmail.com
 */
public class DistributedLockException extends RuntimeException {

    /**
     * constructor with error details
     * @param message the error message
     */
    public DistributedLockException(String message) {
        super(message);
    }

    /**
     * constructor with error exception
     * @param e the throwable
     */
    public DistributedLockException(Throwable e) {
        super(e);
    }
}
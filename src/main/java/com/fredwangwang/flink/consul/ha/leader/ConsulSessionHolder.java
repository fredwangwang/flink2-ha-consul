/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
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

package com.fredwangwang.flink.consul.ha.leader;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the current Consul session ID used for leader latch. Session is created and renewed
 * by the leader latch; when it expires or is destroyed, the key is released.
 */
public final class ConsulSessionHolder {

    private final AtomicReference<String> sessionId = new AtomicReference<>(null);

    public String getSessionId() {
        return sessionId.get();
    }

    public void setSessionId(String id) {
        sessionId.set(id);
    }

    boolean hasSession() {
        return sessionId.get() != null && !sessionId.get().isEmpty();
    }

    boolean compareSessionId(String other) {
        String current = sessionId.get();
        return (current == null && other == null) || (current != null && current.equals(other));
    }
}

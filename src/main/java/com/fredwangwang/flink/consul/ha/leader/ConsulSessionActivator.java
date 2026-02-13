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

import com.fredwangwang.flink.consul.ha.VertxConsulClientAdapter;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Creates and renews a Consul session. The session is used to bind the leader latch key;
 * when the session expires or is destroyed, the key is released.
 */
public final class ConsulSessionActivator {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulSessionActivator.class);

    private final VertxConsulClientAdapter client;
    private final Executor executor;
    private final int sessionTtlSeconds;
    private final long lockDelaySeconds;
    private volatile boolean running;
    private final ConsulSessionHolder holder;

    public ConsulSessionActivator(
            VertxConsulClientAdapter client,
            Executor executor,
            int sessionTtlSeconds,
            long lockDelaySeconds,
            ConsulSessionHolder holder) {
        this.client = Preconditions.checkNotNull(client);
        this.executor = Preconditions.checkNotNull(executor);
        this.sessionTtlSeconds = Math.max(10, sessionTtlSeconds);
        this.lockDelaySeconds = Math.max(0, lockDelaySeconds);
        this.holder = Preconditions.checkNotNull(holder);
    }

    public void start() {
        running = true;
        executor.execute(this::run);
    }

    public void stop() {
        running = false;
    }

    private void run() {
        LOG.debug("ConsulSessionActivator started");
        while (running) {
            try {
                if (holder.getSessionId() == null || holder.getSessionId().isEmpty()) {
                    createSession();
                } else {
                    renewSession();
                }
            } catch (Exception e) {
                LOG.warn("Consul session create/renew failed, will retry", e);
                holder.setSessionId(null);
            }
            if (!running) break;
            try {
                Thread.sleep(1000L * (sessionTtlSeconds / 2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        destroySession();
        LOG.debug("ConsulSessionActivator stopped");
    }

    private void createSession() {
        String id = client.sessionCreate("flink-ha", sessionTtlSeconds, lockDelaySeconds);
        if (id != null) {
            holder.setSessionId(id);
            LOG.debug("Consul session created: {}", id);
        }
    }

    private void renewSession() {
        String id = holder.getSessionId();
        if (id == null || id.isEmpty()) return;
        try {
            client.renewSession(id);
            LOG.trace("Consul session renewed: {}", id);
        } catch (Exception e) {
            LOG.warn("Consul session renew failed, session may be invalid", e);
            holder.setSessionId(null);
        }
    }

    private void destroySession() {
        String id = holder.getSessionId();
        if (id == null || id.isEmpty()) return;
        try {
            client.sessionDestroy(id);
            LOG.debug("Consul session destroyed: {}", id);
        } catch (Exception e) {
            LOG.warn("Consul session destroy failed", e);
        }
        holder.setSessionId(null);
    }
}

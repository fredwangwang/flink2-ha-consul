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
import com.fredwangwang.flink.consul.ha.ConsulUtils;
import com.fredwangwang.flink.consul.ha.configuration.ConsulHighAvailabilityOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consul-based {@link LeaderElectionDriver}. Uses a Consul session and KV key (leader latch)
 * for leader election; writes component leader information to separate KV keys.
 */
public class ConsulLeaderElectionDriver implements LeaderElectionDriver {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderElectionDriver.class);

    private final VertxConsulClientAdapter client;
    private final Configuration configuration;
    private final LeaderElectionDriver.Listener listener;
    private final Executor executor;
    private final String leaderLatchKey;
    private final ConsulSessionHolder sessionHolder;
    private final ConsulSessionActivator sessionActivator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean hasLeadership;

    public ConsulLeaderElectionDriver(
            VertxConsulClientAdapter client,
            Configuration configuration,
            LeaderElectionDriver.Listener listener,
            Executor executor) {
        this.client = Preconditions.checkNotNull(client);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.listener = Preconditions.checkNotNull(listener);
        this.executor = Preconditions.checkNotNull(executor);
        this.leaderLatchKey = ConsulUtils.getLeaderLatchKey(configuration);
        this.sessionHolder = new ConsulSessionHolder();
        int sessionTtl = (int) configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL).getSeconds();
        this.sessionActivator = new ConsulSessionActivator(client, executor, sessionTtl, sessionHolder);
        sessionActivator.start();
        executor.execute(this::leaderLatchLoop);
    }

    @Override
    public boolean hasLeadership() {
        return hasLeadership;
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        if (!running.get() || !hasLeadership) return;
        String key = ConsulUtils.getConnectionInfoKey(configuration, componentId);
        byte[] data = LeaderInformationCodec.toBytes(leaderInformation);
        try {
            if (data == null || data.length == 0) {
                client.deleteKVValue(key);
            } else {
                client.setKVBinaryValue(key, data);
            }
        } catch (Exception e) {
            listener.onError(e);
        }
    }

    @Override
    public void deleteLeaderInformation(String componentId) {
        try {
            String key = ConsulUtils.getConnectionInfoKey(configuration, componentId);
            client.deleteKVValue(key);
        } catch (Exception e) {
            listener.onError(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (!running.compareAndSet(true, false)) return;
        LOG.info("Closing ConsulLeaderElectionDriver");
        sessionActivator.stop();
        releaseLatchKey();
        Exception ex = null;
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ExceptionUtils.tryRethrowException(ex);
    }

    private void leaderLatchLoop() {
        while (running.get()) {
            try {
                String sessionId = sessionHolder.getSessionId();
                if (sessionId == null || sessionId.isEmpty()) {
                    if (hasLeadership) {
                        hasLeadership = false;
                        listener.onRevokeLeadership();
                    }
                    Thread.sleep(1000);
                    continue;
                }
                boolean acquired = tryAcquireLatchKey(sessionId);
                if (acquired && !hasLeadership) {
                    hasLeadership = true;
                    UUID leaderSessionId = UUID.randomUUID();
                    LOG.info("Consul leadership acquired with session {}", leaderSessionId);
                    listener.onGrantLeadership(leaderSessionId);
                } else if (!acquired && hasLeadership) {
                    hasLeadership = false;
                    LOG.info("Consul leadership lost");
                    listener.onRevokeLeadership();
                }
                if (acquired) {
                    long ttl = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL).getSeconds();
                    Thread.sleep(1000L * Math.max(1, ttl / 3));
                } else {
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.warn("Leader latch loop error", e);
                listener.onError(e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private boolean tryAcquireLatchKey(String sessionId) {
        try {
            Boolean acquired = client.setKVBinaryValue(leaderLatchKey, new byte[]{1}, null, sessionId, null);
            return Boolean.TRUE.equals(acquired);
        } catch (Exception e) {
            return false;
        }
    }

    private void releaseLatchKey() {
        String sessionId = sessionHolder.getSessionId();
        if (sessionId == null || sessionId.isEmpty()) return;
        try {
            client.setKVBinaryValue(leaderLatchKey, new byte[0], null, null, sessionId);
        } catch (Exception e) {
            LOG.warn("Failed to release leader latch key", e);
        }
    }
}

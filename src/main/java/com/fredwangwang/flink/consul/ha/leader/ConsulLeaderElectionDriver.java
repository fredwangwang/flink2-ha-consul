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

import java.util.List;
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
        long lockDelay = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_LOCK_DELAY).getSeconds();
        this.sessionActivator = new ConsulSessionActivator(client, executor, sessionTtl, lockDelay, sessionHolder);
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
        // Delete the leader latch key entirely (not just release).
        // This mirrors ZooKeeper ephemeral node behavior: when the leader closes,
        // the latch key is removed, causing followers attempting to acquire leadership
        // to fail with errors, triggering cluster-wide shutdown in application mode.
        deleteLeaderLatchKey();
        Exception ex = null;
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ExceptionUtils.tryRethrowException(ex);
    }

    private void leaderLatchLoop() {
        // Track whether we've ever seen cluster data (latch key or any other HA keys).
        // This helps distinguish initial cluster startup (no data yet)
        // from cluster shutdown (data was deleted by cleanup).
        boolean clusterDataSeenExists = false;
        
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
                
                // Check if any cluster data exists under the base path.
                boolean dataExists = clusterDataExists();
                
                if (dataExists) {
                    clusterDataSeenExists = true;
                }
                
                if (clusterDataSeenExists && !dataExists) {
                    // We previously saw cluster data (latch key or other keys), but now 
                    // there's no cluster data at all. This means internalCleanup() was called.
                    // This mimics ZooKeeper behavior where followers fail with NoNodeException
                    // when parent paths are deleted during cleanup, triggering cluster shutdown.
                    LOG.info("Cluster HA data no longer exists after being seen previously. " +
                            "Cluster has been cleaned up, shutting down.");
                    listener.onError(new Exception(
                        "Cluster HA data under base path no longer exists. " +
                        "Cluster has been cleaned up, shutting down."));
                    break;
                }
                
                if (!clusterDataSeenExists) {
                    // No cluster data seen yet - this is initial startup.
                    // We can try to become the leader and create the latch key.
                    LOG.info("No cluster data seen yet, initiate new cluster.");
                }
                
                boolean acquired = tryAcquireLatchKey(sessionId);
                if (acquired && !hasLeadership) {
                    clusterDataSeenExists = true; // If we acquired, key definitely exists now
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

    /**
     * Deletes the leader latch key entirely. This is called during close() to ensure
     * that followers cannot re-acquire leadership after the leader shuts down.
     * This mimics ZooKeeper's ephemeral node behavior where nodes are deleted when
     * the session ends, causing followers to receive errors when accessing the paths.
     */
    private void deleteLeaderLatchKey() {
        try {
            client.deleteKVValue(leaderLatchKey);
            LOG.debug("Deleted leader latch key: {}", leaderLatchKey);
        } catch (Exception e) {
            LOG.warn("Failed to delete leader latch key", e);
        }
    }
    
    /**
     * Checks if any keys exist under the cluster base path.
     * Used to detect if internalCleanup() was called, which deletes ALL keys.
     * This handles the edge case where a follower starts after cleanup has already occurred.
     */
    private boolean clusterDataExists() {
        try {
            String basePath = ConsulUtils.getConsulBasePath(configuration);
            List<String> keys = client.getKVKeysOnly(basePath.isEmpty() ? "/" : basePath + "/");
            return keys != null && !keys.isEmpty();
        } catch (Exception e) {
            LOG.debug("Error checking cluster data existence", e);
            return false;
        }
    }
}

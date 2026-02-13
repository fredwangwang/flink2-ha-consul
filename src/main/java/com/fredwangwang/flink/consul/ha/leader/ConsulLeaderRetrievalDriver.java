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

import com.fredwangwang.flink.consul.ha.ConsulUtils;
import com.fredwangwang.flink.consul.ha.VertxConsulClientAdapter;
import com.fredwangwang.flink.consul.ha.configuration.ConsulHighAvailabilityOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consul-based {@link LeaderRetrievalDriver}. Uses Consul blocking query (long polling) on the
 * connection info key for a component and notifies the handler when leader information changes.
 */
public class ConsulLeaderRetrievalDriver implements LeaderRetrievalDriver {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderRetrievalDriver.class);

    private static final long RETRY_INITIAL_DELAY_MS = 1_000L;
    private static final long RETRY_MAX_DELAY_MS = 5_000L;
    private static final double RETRY_BACKOFF_MULTIPLIER = 2.0;

    private final VertxConsulClientAdapter client;
    private final String connectionInfoKey;
    private final LeaderRetrievalEventHandler eventHandler;
    private final FatalErrorHandler fatalErrorHandler;
    private final Executor executor;
    private final int waitTimeSeconds;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ConsulLeaderRetrievalDriver(
            VertxConsulClientAdapter client,
            Configuration configuration,
            String componentId,
            LeaderRetrievalEventHandler eventHandler,
            FatalErrorHandler fatalErrorHandler,
            Executor executor) {
        this.client = Preconditions.checkNotNull(client);
        this.connectionInfoKey = ConsulUtils.getConnectionInfoKey(configuration, componentId);
        this.eventHandler = Preconditions.checkNotNull(eventHandler);
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
        this.executor = Preconditions.checkNotNull(executor);
        this.waitTimeSeconds = (int) configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_BLOCKING_QUERY_WAIT).getSeconds();
    }

    @Override
    public void close() throws Exception {
        if (!running.compareAndSet(true, false)) return;
        LOG.debug("Closing ConsulLeaderRetrievalDriver for key {}", connectionInfoKey);
    }

    /** Starts watching the connection info key; call from the service after construction. */
    void start() {
        executor.execute(this::watchLoop);
    }

    private void watchLoop() {
        long index = 0;
        long retryDelayMs = RETRY_INITIAL_DELAY_MS;
        while (running.get()) {
            try {
                VertxConsulClientAdapter.BinaryKeyValue value = client.getKVBinaryValue(connectionInfoKey, index, waitTimeSeconds);
                if (!running.get()) break;
                retryDelayMs = RETRY_INITIAL_DELAY_MS; // reset backoff on success
                if (value != null) {
                    index = value.getModifyIndex();
                    byte[] data = value.getValue();
                    LeaderInformation info = LeaderInformationCodec.fromBytes(data);
                    eventHandler.notifyLeaderAddress(info);
                } else {
                    eventHandler.notifyLeaderAddress(LeaderInformation.empty());
                }
            } catch (Exception e) {
                if (!running.get()) break;
                ExceptionUtils.checkInterrupted(e);
                if (ConsulUtils.isTransientConsulFailure(e)) {
                    LOG.warn(
                            "Connection to Consul suspended, waiting for reconnection. Key: {}, error: {}",
                            connectionInfoKey,
                            e.getMessage());
                    LOG.debug("Consul leader retrieval transient failure", e);
                    eventHandler.notifyLeaderAddress(LeaderInformation.empty());
                    try {
                        Thread.sleep(retryDelayMs);
                        retryDelayMs = Math.min((long) (retryDelayMs * RETRY_BACKOFF_MULTIPLIER), RETRY_MAX_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    fatalErrorHandler.onFatalError(new LeaderRetrievalException("Consul leader retrieval failed", e));
                    break;
                }
            }
        }
    }
}

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

package com.fredwangwang.flink.consul.ha;

import com.ecwid.consul.v1.ConsulClient;
import com.fredwangwang.flink.consul.ha.leader.ConsulLeaderRetrievalDriverFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import javax.annotation.Nonnull;

/**
 * Consul-based {@link ClientHighAvailabilityServices}. Provides the cluster REST endpoint
 * leader retriever only; uses a single-thread executor for the retrieval driver.
 */
public class ConsulClientHAServices implements ClientHighAvailabilityServices {

    private final ConsulClient client;
    private final Configuration configuration;

    public ConsulClientHAServices(
            @Nonnull ConsulClient client,
            @Nonnull Configuration configuration) {
        this.client = Preconditions.checkNotNull(client);
        this.configuration = Preconditions.checkNotNull(configuration);
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return new DefaultLeaderRetrievalService(
                new ConsulLeaderRetrievalDriverFactory(
                        client,
                        configuration,
                        ConsulUtils.getRestServerNode(),
                        Executors.newDirectExecutorService()));
    }

    @Override
    public void close() throws Exception {
        // ConsulClient has no close(); connection is not long-lived.
    }
}

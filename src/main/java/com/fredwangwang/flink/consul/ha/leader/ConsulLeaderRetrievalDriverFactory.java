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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

/** {@link LeaderRetrievalDriverFactory} for Consul. */
public class ConsulLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final VertxConsulClientAdapter client;
    private final Configuration configuration;
    private final String componentId;
    private final Executor executor;

    public ConsulLeaderRetrievalDriverFactory(
            VertxConsulClientAdapter client,
            Configuration configuration,
            String componentId,
            Executor executor) {
        this.client = Preconditions.checkNotNull(client);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.componentId = Preconditions.checkNotNull(componentId);
        this.executor = Preconditions.checkNotNull(executor);
    }

    @Override
    public LeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler) throws Exception {
        ConsulLeaderRetrievalDriver driver = new ConsulLeaderRetrievalDriver(
                client, configuration, componentId, leaderEventHandler, fatalErrorHandler, executor);
        driver.start();
        return driver;
    }
}

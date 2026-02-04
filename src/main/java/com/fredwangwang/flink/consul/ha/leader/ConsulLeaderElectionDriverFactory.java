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

import com.ecwid.consul.v1.ConsulClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

/** Factory for {@link ConsulLeaderElectionDriver}. */
public class ConsulLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

    private final ConsulClient client;
    private final Configuration configuration;
    private final Executor executor;

    public ConsulLeaderElectionDriverFactory(
            ConsulClient client, Configuration configuration, Executor executor) {
        this.client = Preconditions.checkNotNull(client);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.executor = Preconditions.checkNotNull(executor);
    }

    @Override
    public LeaderElectionDriver create(LeaderElectionDriver.Listener leaderElectionListener) throws Exception {
        return new ConsulLeaderElectionDriver(client, configuration, leaderElectionListener, executor);
    }
}

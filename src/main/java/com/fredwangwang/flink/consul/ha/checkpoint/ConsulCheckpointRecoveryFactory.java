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

package com.fredwangwang.flink.consul.ha.checkpoint;

import com.ecwid.consul.v1.ConsulClient;
import com.fredwangwang.flink.consul.ha.ConsulUtils;
import com.fredwangwang.flink.consul.ha.leader.ConsulSessionHolder;
import com.fredwangwang.flink.consul.ha.store.ConsulKVStateHandleStore;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

import static org.apache.flink.runtime.util.ZooKeeperUtils.HA_STORAGE_COMPLETED_CHECKPOINT;
import static org.apache.flink.runtime.util.ZooKeeperUtils.createFileSystemStateStorage;

/**
 * Consul-backed {@link CheckpointRecoveryFactory}. Uses Consul KV for state handle pointers and
 * file system (HA_STORAGE_PATH) for actual checkpoint state.
 */
public class ConsulCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

    private final ConsulClient client;
    private final Configuration config;
    private final Executor executor;
    private final ConsulSessionHolder sessionHolder;

    public ConsulCheckpointRecoveryFactory(
            ConsulClient client,
            Configuration config,
            Executor executor,
            ConsulSessionHolder sessionHolder) {
        this.client = Preconditions.checkNotNull(client);
        this.config = Preconditions.checkNotNull(config);
        this.executor = Preconditions.checkNotNull(executor);
        this.sessionHolder = Preconditions.checkNotNull(sessionHolder);
    }

    @Override
    public org.apache.flink.runtime.checkpoint.CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RecoveryClaimMode recoveryClaimMode)
            throws Exception {
        RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
                createFileSystemStateStorage(config, HA_STORAGE_COMPLETED_CHECKPOINT);
        String checkpointsPath = ConsulUtils.getCheckpointsPath(config, jobId);
        ConsulKVStateHandleStore<CompletedCheckpoint> store =
                new ConsulKVStateHandleStore<>(client, checkpointsPath, stateStorage, sessionHolder);
        org.apache.flink.runtime.checkpoint.CheckpointStoreUtil util = ConsulCheckpointStoreUtil.INSTANCE;
        var completedCheckpoints = DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(store, util);
        return new DefaultCompletedCheckpointStore<>(
                maxNumberOfCheckpointsToRetain,
                store,
                util,
                completedCheckpoints,
                sharedStateRegistryFactory.create(ioExecutor, completedCheckpoints, recoveryClaimMode),
                executor);
    }

    @Override
    public org.apache.flink.runtime.checkpoint.CheckpointIDCounter createCheckpointIDCounter(JobID jobID) throws Exception {
        ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(client, config, jobID);
        counter.start();
        return counter;
    }
}

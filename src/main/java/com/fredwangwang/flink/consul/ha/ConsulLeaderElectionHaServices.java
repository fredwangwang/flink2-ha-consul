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

import com.fredwangwang.flink.consul.ha.checkpoint.ConsulCheckpointRecoveryFactory;
import com.fredwangwang.flink.consul.ha.configuration.ConsulHighAvailabilityOptions;
import com.fredwangwang.flink.consul.ha.executionplan.ConsulExecutionPlanStoreUtil;
import com.fredwangwang.flink.consul.ha.executionplan.ConsulExecutionPlanStoreWatcher;
import com.fredwangwang.flink.consul.ha.leader.ConsulLeaderElectionDriverFactory;
import com.fredwangwang.flink.consul.ha.leader.ConsulLeaderRetrievalDriverFactory;
import com.fredwangwang.flink.consul.ha.leader.ConsulSessionActivator;
import com.fredwangwang.flink.consul.ha.leader.ConsulSessionHolder;
import com.fredwangwang.flink.consul.ha.store.ConsulKVStateHandleStore;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.jobmanager.DefaultExecutionPlanStore;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.util.ZooKeeperUtils.HA_STORAGE_SUBMITTED_EXECUTION_PLAN_PREFIX;
import static org.apache.flink.runtime.util.ZooKeeperUtils.createFileSystemStateStorage;

/**
 * Consul-based HA services: leader election/retrieval via Consul KV and sessions;
 * checkpoints and execution plans use Consul KV for handles and file system (HA_STORAGE_PATH) for state.
 */
public class ConsulLeaderElectionHaServices extends AbstractHaServices {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderElectionHaServices.class);

    private final VertxConsulClientAdapter client;
    private final ConsulSessionHolder stateStoreSessionHolder;
    private final ConsulSessionActivator stateStoreSessionActivator;

    public ConsulLeaderElectionHaServices(
            VertxConsulClientAdapter client,
            Configuration configuration,
            Executor executor,
            BlobStoreService blobStoreService) throws Exception {
        super(
                configuration,
                new ConsulLeaderElectionDriverFactory(client, configuration, executor),
                executor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(configuration, executor));
        this.client = Preconditions.checkNotNull(client);
        this.stateStoreSessionHolder = new ConsulSessionHolder();
        int sessionTtl = (int) configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL).getSeconds();
        this.stateStoreSessionActivator = new ConsulSessionActivator(
                client, executor, sessionTtl, stateStoreSessionHolder);
        stateStoreSessionActivator.start();
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String componentId) {
        return new DefaultLeaderRetrievalService(
                new ConsulLeaderRetrievalDriverFactory(client, configuration, componentId, ioExecutor));
    }

    @Override
    protected CheckpointRecoveryFactory createCheckpointRecoveryFactory() throws Exception {
        return new ConsulCheckpointRecoveryFactory(
                client, configuration, ioExecutor, stateStoreSessionHolder);
    }

    @Override
    protected ExecutionPlanStore createExecutionPlanStore() throws Exception {
        org.apache.flink.runtime.persistence.RetrievableStateStorageHelper<ExecutionPlan> stateStorage =
                createFileSystemStateStorage(configuration, HA_STORAGE_SUBMITTED_EXECUTION_PLAN_PREFIX);
        String basePath = ConsulUtils.getExecutionPlansPath(configuration);
        ConsulKVStateHandleStore<ExecutionPlan> store = new ConsulKVStateHandleStore<>(
                client, basePath, stateStorage, stateStoreSessionHolder);
        ConsulExecutionPlanStoreWatcher watcher = new ConsulExecutionPlanStoreWatcher(client, configuration, ioExecutor);
        return new DefaultExecutionPlanStore<>(store, watcher, ConsulExecutionPlanStoreUtil.INSTANCE);
    }

    @Override
    protected void internalClose() throws Exception {
        stateStoreSessionActivator.stop();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.close();
    }

    @Override
    protected void internalCleanup() throws Exception {
        String basePath = ConsulUtils.getConsulBasePath(configuration);
        deleteKeysRecursive(basePath);
    }

    @Override
    protected void internalCleanupJobData(JobID jobID) throws Exception {
        String leaderPath = ConsulUtils.getLeaderPathForJobKey(configuration, jobID);
        client.deleteKVValue(leaderPath);
        String jobPath = ConsulUtils.getJobPath(configuration, jobID);
        deleteKeysRecursive(jobPath);
    }

    private void deleteKeysRecursive(String prefix) {
        try {
            List<String> keys = client.getKVKeysOnly(prefix.isEmpty() ? "/" : prefix + "/");
            if (keys == null) return;
            for (String key : keys) {
                client.deleteKVValue(key);
            }
            client.deleteKVValue(prefix);
        } catch (Exception e) {
            LOG.warn("Failed to delete Consul keys under {}", prefix, e);
        }
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return ConsulUtils.getResourceManagerNode();
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return ConsulUtils.getDispatcherNode();
    }

    @Override
    protected String getLeaderPathForJobManager(JobID jobID) {
        return ConsulUtils.getLeaderPathForJob(jobID);
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return ConsulUtils.getRestServerNode();
    }
}

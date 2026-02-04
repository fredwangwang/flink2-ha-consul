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

package com.fredwangwang.flink.consul.ha.executionplan;

import com.fredwangwang.flink.consul.ha.ConsulUtils;
import com.fredwangwang.flink.consul.ha.VertxConsulClientAdapter;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches Consul KV keys under the execution plans path and notifies the store of add/remove
 * via polling (Consul has no watch API for key prefix changes; blocking query is per-key).
 */
public class ConsulExecutionPlanStoreWatcher implements org.apache.flink.runtime.jobmanager.ExecutionPlanStoreWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulExecutionPlanStoreWatcher.class);

    private final VertxConsulClientAdapter client;
    private final String basePath;
    private final Executor executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ExecutionPlanStore.ExecutionPlanListener listener;
    private volatile Set<String> lastKeys = new HashSet<>();

    public ConsulExecutionPlanStoreWatcher(
            VertxConsulClientAdapter client,
            Configuration configuration,
            Executor executor) {
        this.client = Preconditions.checkNotNull(client);
        this.basePath = ConsulUtils.getExecutionPlansPath(configuration);
        this.executor = Preconditions.checkNotNull(executor);
    }

    @Override
    public void start(ExecutionPlanStore.ExecutionPlanListener executionPlanListener) throws Exception {
        this.listener = Preconditions.checkNotNull(executionPlanListener);
        this.running.set(true);
        this.lastKeys = new HashSet<>();
        executor.execute(this::pollLoop);
    }

    @Override
    public void stop() throws Exception {
        running.set(false);
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                List<String> keys = client.getKVKeysOnly(
                        basePath.isEmpty() ? "/" : basePath + "/");
                Set<String> current = new HashSet<>();
                if (keys != null) {
                    for (String fullKey : keys) {
                        if (fullKey.contains("lock/")) continue;
                        String name = fullKey.contains("/")
                                ? fullKey.substring(fullKey.lastIndexOf('/') + 1)
                                : fullKey;
                        if (!basePath.isEmpty() && fullKey.startsWith(basePath + "/")) {
                            name = fullKey.substring((basePath + "/").length());
                        }
                        current.add(name);
                    }
                }
                ExecutionPlanStore.ExecutionPlanListener l = listener;
                if (l != null) {
                    for (String name : current) {
                        if (!lastKeys.contains(name)) {
                            try {
                                l.onAddedExecutionPlan(JobID.fromHexString(name));
                            } catch (Exception e) {
                                LOG.debug("Could not parse job id from name {}", name, e);
                            }
                        }
                    }
                    for (String name : lastKeys) {
                        if (!current.contains(name)) {
                            try {
                                l.onRemovedExecutionPlan(JobID.fromHexString(name));
                            } catch (Exception e) {
                                LOG.debug("Could not parse job id from name {}", name, e);
                            }
                        }
                    }
                }
                lastKeys = current;
            } catch (Exception e) {
                if (running.get()) {
                    LOG.warn("Consul execution plan watcher error", e);
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

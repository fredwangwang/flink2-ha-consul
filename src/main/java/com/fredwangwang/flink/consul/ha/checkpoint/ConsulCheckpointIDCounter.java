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
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.fredwangwang.flink.consul.ha.ConsulUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Consul-backed {@link CheckpointIDCounter}. Stores a single long value in Consul KV and uses CAS
 * for atomic getAndIncrement.
 */
public class ConsulCheckpointIDCounter implements CheckpointIDCounter {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulCheckpointIDCounter.class);

    private final ConsulClient client;
    private final String key;
    private volatile long currentCount = INITIAL_CHECKPOINT_ID - 1;

    public ConsulCheckpointIDCounter(ConsulClient client, Configuration configuration, JobID jobID) {
        this.client = Preconditions.checkNotNull(client);
        this.key = ConsulUtils.getCheckpointIdCounterKey(configuration, jobID);
    }

    @Override
    public void start() throws Exception {
        Response<GetBinaryValue> r = client.getKVBinaryValue(key, QueryParams.DEFAULT);
        GetBinaryValue v = r.getValue();
        if (v != null && v.getValue() != null && v.getValue().length >= 8) {
            currentCount = ByteBuffer.wrap(v.getValue()).getLong();
        }
    }

    @Override
    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
        if (jobStatus.isGloballyTerminalState()) {
            try {
                client.deleteKVValue(key);
            } catch (Exception e) {
                LOG.warn("Failed to delete checkpoint counter key {}", key, e);
                return FutureUtils.completedExceptionally(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getAndIncrement() throws Exception {
        while (true) {
            Response<GetBinaryValue> r = client.getKVBinaryValue(key, QueryParams.DEFAULT);
            GetBinaryValue v = r.getValue();
            long prev = INITIAL_CHECKPOINT_ID - 1;
            long index = 0;
            if (v != null && v.getValue() != null && v.getValue().length >= 8) {
                prev = ByteBuffer.wrap(v.getValue()).getLong();
                index = v.getModifyIndex();
            }
            long next = prev + 1;
            if (next > Integer.MAX_VALUE) {
                throw new IllegalStateException("Checkpoint counter overflow");
            }
            byte[] bytes = ByteBuffer.allocate(8).putLong(next).array();
            PutParams params = new PutParams();
            if (index > 0) {
                params.setCas(index);
            }
            Boolean ok = client.setKVBinaryValue(key, bytes, params).getValue();
            if (Boolean.TRUE.equals(ok)) {
                currentCount = next;
                return prev;
            }
        }
    }

    @Override
    public long get() {
        try {
            Response<GetBinaryValue> r = client.getKVBinaryValue(key, QueryParams.DEFAULT);
            GetBinaryValue v = r.getValue();
            if (v != null && v.getValue() != null && v.getValue().length >= 8) {
                return ByteBuffer.wrap(v.getValue()).getLong();
            }
        } catch (Exception e) {
            LOG.debug("Could not get checkpoint counter", e);
        }
        return currentCount;
    }

    @Override
    public void setCount(long newId) throws Exception {
        if (newId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Checkpoint ID exceeds Integer.MAX_VALUE");
        }
        byte[] bytes = ByteBuffer.allocate(8).putLong(newId).array();
        client.setKVBinaryValue(key, bytes);
        currentCount = newId;
    }
}

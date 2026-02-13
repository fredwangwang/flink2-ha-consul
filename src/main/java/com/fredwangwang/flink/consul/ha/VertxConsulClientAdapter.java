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

import io.vertx.core.Vertx;
import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.SessionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Sync adapter around Vert.x Consul client. Blocks on Futures so existing Flink code
 * can use a sync API. Supports ACL token via {@link io.vertx.ext.consul.ConsulClientOptions}.
 * Binary values are stored as Base64 in Consul (Consul API uses base64 for values).
 */
public final class VertxConsulClientAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(VertxConsulClientAdapter.class);
    private static final long BLOCKING_TIMEOUT_SECONDS = 60;

    private final Vertx vertx;
    private final ConsulClient consulClient;

    public VertxConsulClientAdapter(Vertx vertx, ConsulClient consulClient) {
        this.vertx = vertx;
        this.consulClient = consulClient;
    }

    /** Result of a binary KV get: value bytes and modify index. */
    public static final class BinaryKeyValue {
        private final byte[] value;
        private final long modifyIndex;

        public BinaryKeyValue(byte[] value, long modifyIndex) {
            this.value = value;
            this.modifyIndex = modifyIndex;
        }

        public byte[] getValue() {
            return value;
        }

        public long getModifyIndex() {
            return modifyIndex;
        }
    }

    /** Get binary value for a key. Returns null if key does not exist or value is empty. */
    public BinaryKeyValue getKVBinaryValue(String key) {
        try {
            KeyValue kv = consulClient.getValue(key)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return kv != null ? keyValueToBinary(kv) : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted getting key " + key, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get key " + key, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout getting key " + key, e);
        }
    }

    /** Get binary value with blocking query (long polling). */
    public BinaryKeyValue getKVBinaryValue(String key, long index, int waitSeconds) {
        BlockingQueryOptions opts = new BlockingQueryOptions()
                .setIndex(index)
                .setWait(waitSeconds + "s");
        try {
            KeyValue kv = consulClient.getValueWithOptions(key, opts)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS + waitSeconds, TimeUnit.SECONDS);
            return kv != null ? keyValueToBinary(kv) : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted getting key " + key, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get key " + key, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout getting key " + key, e);
        }
    }

    private BinaryKeyValue keyValueToBinary(KeyValue kv) {
        String v = kv.getValue();
        if (v == null || v.isEmpty()) {
            return new BinaryKeyValue(null, kv.getModifyIndex());
        }
        byte[] decoded = Base64.getDecoder().decode(v.getBytes(StandardCharsets.UTF_8));
        return new BinaryKeyValue(decoded, kv.getModifyIndex());
    }

    private static String bytesToBase64(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    /** Set binary value (no CAS or session). */
    public void setKVBinaryValue(String key, byte[] value) {
        try {
            consulClient.putValue(key, bytesToBase64(value))
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted putting key " + key, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to put key " + key, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout putting key " + key, e);
        }
    }

    /**
     * Set binary value with optional CAS and/or session acquire/release.
     * Returns true if the operation succeeded (for CAS: index matched; for acquire: lock acquired).
     */
    public Boolean setKVBinaryValue(String key, byte[] value, Long casIndex, String acquireSession, String releaseSession) {
        KeyValueOptions opts = new KeyValueOptions();
        if (casIndex != null && casIndex > 0) {
            opts.setCasIndex(casIndex);
        }
        if (acquireSession != null && !acquireSession.isEmpty()) {
            opts.setAcquireSession(acquireSession);
        }
        if (releaseSession != null && !releaseSession.isEmpty()) {
            opts.setReleaseSession(releaseSession);
        }
        try {
            return consulClient.putValueWithOptions(key, bytesToBase64(value), opts)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted putting key " + key, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to put key " + key, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout putting key " + key, e);
        }
    }

    public void deleteKVValue(String key) {
        try {
            consulClient.deleteValue(key)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted deleting key " + key, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to delete key " + key, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout deleting key " + key, e);
        }
    }

    /** Get keys under prefix (keys only). Prefix should not have leading slash; use empty or "/" for root. */
    public List<String> getKVKeysOnly(String prefix) {
        String keyPrefix = (prefix == null || prefix.isEmpty() || "/".equals(prefix)) ? "" : prefix.endsWith("/") ? prefix : prefix + "/";
        try {
            KeyValueList list = consulClient.getValues(keyPrefix.isEmpty() ? "/" : keyPrefix)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (list == null || list.getList() == null) {
                return Collections.emptyList();
            }
            return list.getList().stream()
                    .map(KeyValue::getKey)
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted listing keys " + prefix, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list keys " + prefix, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout listing keys " + prefix, e);
        }
    }

    /** Create a session; returns session ID. */
    public String sessionCreate(String name, int ttlSeconds) {
        return sessionCreate(name, ttlSeconds, 0);
    }

    /** Create a session with optional lock delay; returns session ID. */
    public String sessionCreate(String name, int ttlSeconds, long lockDelaySeconds) {
        SessionOptions opts = new SessionOptions()
                .setName(name)
                .setTtl((long) ttlSeconds);
        if (lockDelaySeconds > 0) {
            opts.setLockDelay(lockDelaySeconds);
        }
        try {
            return consulClient.createSessionWithOptions(opts)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted creating session", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create session", e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout creating session", e);
        }
    }

    public void renewSession(String sessionId) {
        try {
            consulClient.renewSession(sessionId)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted renewing session", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to renew session", e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout renewing session", e);
        }
    }

    public void sessionDestroy(String sessionId) {
        try {
            consulClient.destroySession(sessionId)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted destroying session", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to destroy session", e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout destroying session", e);
        }
    }

    /** Close the adapter and release Vert.x. Call when HA services are closed. */
    public void close() {
        try {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("Error closing Vert.x", e);
        }
    }
}

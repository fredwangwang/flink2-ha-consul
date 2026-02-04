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

package com.fredwangwang.flink.consul.ha.store;

import com.fredwangwang.flink.consul.ha.ConsulUtils;
import com.fredwangwang.flink.consul.ha.VertxConsulClientAdapter;
import com.fredwangwang.flink.consul.ha.leader.ConsulSessionHolder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.util.StateHandleStoreUtils.deserialize;
import static org.apache.flink.runtime.util.StateHandleStoreUtils.serializeOrDiscard;

/**
 * Consul KV-backed {@link StateHandleStore}. State is persisted via {@link RetrievableStateStorageHelper}
 * (e.g. file system); only the state handle bytes are stored in Consul. Locking uses Consul
 * session-bound keys (one lock key per handle name).
 */
public class ConsulKVStateHandleStore<T extends Serializable>
        implements StateHandleStore<T, IntegerResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulKVStateHandleStore.class);
    private static final String LOCK_PREFIX = "lock/";

    private final VertxConsulClientAdapter client;
    private final String basePath;
    private final RetrievableStateStorageHelper<T> storage;
    private final ConsulSessionHolder sessionHolder;
    private final Set<String> lockedNames = ConcurrentHashMap.newKeySet();

    public ConsulKVStateHandleStore(
            VertxConsulClientAdapter client,
            String basePath,
            RetrievableStateStorageHelper<T> storage,
            ConsulSessionHolder sessionHolder) {
        this.client = Preconditions.checkNotNull(client);
        this.basePath = basePath == null || basePath.isEmpty() ? "" : ConsulUtils.toKey(basePath);
        this.storage = Preconditions.checkNotNull(storage);
        this.sessionHolder = Preconditions.checkNotNull(sessionHolder);
    }

    private String handleKey(String name) {
        return basePath.isEmpty() ? name : basePath + "/" + name;
    }

    private String lockKey(String name) {
        return basePath.isEmpty() ? LOCK_PREFIX + name : basePath + "/" + LOCK_PREFIX + name;
    }

    @Override
    public RetrievableStateHandle<T> addAndLock(String name, T state)
            throws PossibleInconsistentStateException, Exception {
        RetrievableStateHandle<T> handle = storage.store(state);
        byte[] bytes = serializeOrDiscard(handle);
        String key = handleKey(name);
        try {
            client.setKVBinaryValue(key, bytes);
            String sid = sessionHolder.getSessionId();
            if (sid != null && !sid.isEmpty()) {
                Boolean acquired = client.setKVBinaryValue(lockKey(name), new byte[]{1}, null, sid, null);
                if (Boolean.TRUE.equals(acquired)) {
                    lockedNames.add(name);
                }
            }
            return handle;
        } catch (Exception e) {
            try {
                handle.discardState();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    @Override
    public void replace(String name, IntegerResourceVersion resourceVersion, T state)
            throws PossibleInconsistentStateException, Exception {
        String key = handleKey(name);
        Long casIndex = Long.valueOf(resourceVersion.getValue());
        RetrievableStateHandle<T> handle = storage.store(state);
        byte[] bytes = serializeOrDiscard(handle);
        try {
            Boolean ok = client.setKVBinaryValue(key, bytes, casIndex, null, null);
            if (!Boolean.TRUE.equals(ok)) {
                handle.discardState();
                throw new StateHandleStore.AlreadyExistException("CAS failed for " + name);
            }
        } catch (StateHandleStore.AlreadyExistException e) {
            throw e;
        } catch (Exception e) {
            try {
                handle.discardState();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    @Override
    public IntegerResourceVersion exists(String name) throws Exception {
        VertxConsulClientAdapter.BinaryKeyValue v = client.getKVBinaryValue(handleKey(name));
        if (v == null || v.getValue() == null || v.getValue().length == 0) {
            return IntegerResourceVersion.notExisting();
        }
        long index = v.getModifyIndex();
        return IntegerResourceVersion.valueOf((int) Math.min(index, Integer.MAX_VALUE));
    }

    @Override
    public RetrievableStateHandle<T> getAndLock(String name) throws Exception {
        String key = handleKey(name);
        VertxConsulClientAdapter.BinaryKeyValue v = client.getKVBinaryValue(key);
        if (v == null || v.getValue() == null || v.getValue().length == 0) {
            throw new StateHandleStore.NotExistException("No state handle for " + name);
        }
        @SuppressWarnings("unchecked")
        RetrievableStateHandle<T> handle = deserialize(v.getValue());
        String sid = sessionHolder.getSessionId();
        if (sid != null && !sid.isEmpty()) {
            Boolean acquired = client.setKVBinaryValue(lockKey(name), new byte[]{1}, null, sid, null);
            if (Boolean.TRUE.equals(acquired)) {
                lockedNames.add(name);
            }
        }
        return handle;
    }

    @Override
    public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
        List<String> keys = client.getKVKeysOnly(basePath.isEmpty() ? "/" : basePath + "/");
        if (keys == null) return Collections.emptyList();
        List<Tuple2<RetrievableStateHandle<T>, String>> result = new ArrayList<>();
        for (String fullKey : keys) {
            if (fullKey.contains("/" + LOCK_PREFIX)) continue;
            String name = fullKey.contains("/") ? fullKey.substring(fullKey.lastIndexOf('/') + 1) : fullKey;
            if (basePath != null && !basePath.isEmpty() && fullKey.startsWith(basePath + "/") && !fullKey.contains(LOCK_PREFIX)) {
                name = fullKey.substring((basePath + "/").length());
            }
            try {
                RetrievableStateHandle<T> handle = getAndLock(name);
                result.add(Tuple2.of(handle, name));
            } catch (Exception e) {
                LOG.debug("Could not get handle for {}", name, e);
            }
        }
        return result;
    }

    @Override
    public Collection<String> getAllHandles() throws Exception {
        List<String> keys = client.getKVKeysOnly(basePath.isEmpty() ? "/" : basePath + "/");
        if (keys == null) return Collections.emptyList();
        Set<String> names = new HashSet<>();
        for (String fullKey : keys) {
            if (fullKey.contains(LOCK_PREFIX)) continue;
            String name = fullKey.contains("/") ? fullKey.substring(fullKey.lastIndexOf('/') + 1) : fullKey;
            if (basePath != null && !basePath.isEmpty() && fullKey.startsWith(basePath + "/")) {
                name = fullKey.substring((basePath + "/").length());
            }
            names.add(name);
        }
        return names;
    }

    @Override
    public boolean releaseAndTryRemove(String name) throws Exception {
        release(name);
        String key = handleKey(name);
        String lk = lockKey(name);
        try {
            client.deleteKVValue(lk);
            client.deleteKVValue(key);
            lockedNames.remove(name);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to remove handle {}", name, e);
            return false;
        }
    }

    @Override
    public void clearEntries() throws Exception {
        Collection<String> names = getAllHandles();
        for (String name : names) {
            try {
                client.deleteKVValue(lockKey(name));
                client.deleteKVValue(handleKey(name));
            } catch (Exception e) {
                LOG.warn("Failed to clear entry {}", name, e);
            }
        }
        lockedNames.clear();
    }

    @Override
    public void release(String name) throws Exception {
        if (!lockedNames.contains(name)) return;
        try {
            client.setKVBinaryValue(lockKey(name), new byte[0], null, null, sessionHolder.getSessionId());
        } finally {
            lockedNames.remove(name);
        }
    }

    @Override
    public void releaseAll() throws Exception {
        for (String name : new ArrayList<>(lockedNames)) {
            release(name);
        }
    }
}

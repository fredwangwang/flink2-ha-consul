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

import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;

/** {@link CheckpointStoreUtil} for Consul; same name format as ZooKeeper (padded checkpoint id). */
public enum ConsulCheckpointStoreUtil implements CheckpointStoreUtil {
    INSTANCE;

    @Override
    public String checkpointIDToName(long checkpointId) {
        return String.format("/%019d", checkpointId);
    }

    @Override
    public long nameToCheckpointID(String path) {
        try {
            String numberString = (path != null && path.length() > 0 && path.charAt(0) == '/')
                    ? path.substring(1)
                    : (path != null ? path : "");
            return Long.parseLong(numberString);
        } catch (NumberFormatException e) {
            return INVALID_CHECKPOINT_ID;
        }
    }
}

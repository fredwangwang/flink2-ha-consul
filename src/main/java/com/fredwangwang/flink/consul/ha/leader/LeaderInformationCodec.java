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

import org.apache.flink.runtime.leaderelection.LeaderInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Serialization for leader information compatible with Flink's ZooKeeper format
 * (writeUTF(address) + writeObject(leaderSessionID)).
 */
final class LeaderInformationCodec {

    private LeaderInformationCodec() {}

    static byte[] toBytes(LeaderInformation info) {
        if (info == null || info.isEmpty()) {
            return null;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeUTF(info.getLeaderAddress());
            oos.writeObject(info.getLeaderSessionID());
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LeaderInformation", e);
        }
    }

    static LeaderInformation fromBytes(byte[] data) {
        if (data == null || data.length == 0) {
            return LeaderInformation.empty();
        }
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bais);
            String address = ois.readUTF();
            UUID sessionId = (UUID) ois.readObject();
            return LeaderInformation.known(sessionId, address);
        } catch (IOException | ClassNotFoundException e) {
            return LeaderInformation.empty();
        }
    }
}

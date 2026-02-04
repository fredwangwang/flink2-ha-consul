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

import com.fredwangwang.flink.consul.ha.configuration.ConsulHighAvailabilityOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.api.common.JobID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link ConsulUtils} path and key building. */
class ConsulUtilsTest {

    private static Configuration baseConfig() {
        Configuration c = new Configuration();
        c.set(HighAvailabilityOptions.HA_CLUSTER_ID, "default");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT, "flink");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_LEADER_PATH, "leader");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_JOBS_PATH, "jobs");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_EXECUTION_PLANS_PATH, "execution-plans");
        return c;
    }

    @Test
    void toKeyJoinsSegmentsWithSlash() {
        assertEquals("a/b/c", ConsulUtils.toKey("a", "b", "c"));
        assertEquals("flink/default", ConsulUtils.toKey("flink", "default"));
    }

    @Test
    void toKeyTrimsAndSkipsEmpty() {
        assertEquals("a/c", ConsulUtils.toKey("a", "  ", "c"));
        assertEquals("a", ConsulUtils.toKey("a", "", ""));
    }

    @Test
    void getConsulBasePath() {
        Configuration c = baseConfig();
        assertEquals("flink/default", ConsulUtils.getConsulBasePath(c));
    }

    @Test
    void getLeaderLatchKey() {
        Configuration c = baseConfig();
        assertEquals("flink/default/leader/latch", ConsulUtils.getLeaderLatchKey(c));
    }

    @Test
    void getConnectionInfoKey() {
        Configuration c = baseConfig();
        assertEquals(
                "flink/default/leader/resource_manager/connection_info",
                ConsulUtils.getConnectionInfoKey(c, ConsulUtils.getResourceManagerNode()));
        assertEquals(
                "flink/default/leader/dispatcher/connection_info",
                ConsulUtils.getConnectionInfoKey(c, ConsulUtils.getDispatcherNode()));
        assertEquals(
                "flink/default/leader/rest_server/connection_info",
                ConsulUtils.getConnectionInfoKey(c, ConsulUtils.getRestServerNode()));
    }

    @Test
    void getLeaderPathForJobKey() {
        Configuration c = baseConfig();
        JobID jobId = JobID.fromHexString("a1b2c3d4e5f6789012345678a1b2c3d4");
        assertEquals(
                "flink/default/leader/job-a1b2c3d4e5f6789012345678a1b2c3d4/connection_info",
                ConsulUtils.getLeaderPathForJobKey(c, jobId));
    }

    @Test
    void getExecutionPlanKey() {
        Configuration c = baseConfig();
        JobID jobId = JobID.fromHexString("00000000000000000000000000000001");
        assertEquals(
                "flink/default/execution-plans/00000000000000000000000000000001",
                ConsulUtils.getExecutionPlanKey(c, jobId));
    }

    @Test
    void getCheckpointIdCounterKey() {
        Configuration c = baseConfig();
        JobID jobId = JobID.fromHexString("ffffffffffffffffffffffffffffffff");
        assertEquals(
                "flink/default/jobs/ffffffffffffffffffffffffffffffff/checkpoint_id_counter",
                ConsulUtils.getCheckpointIdCounterKey(c, jobId));
    }

    @Test
    void getCheckpointsPath() {
        Configuration c = baseConfig();
        JobID jobId = JobID.fromHexString("0000000000000000000000000000abcd");
        assertEquals(
                "flink/default/jobs/0000000000000000000000000000abcd/checkpoints",
                ConsulUtils.getCheckpointsPath(c, jobId));
    }
}

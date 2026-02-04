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

package com.fredwangwang.flink.consul.ha.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Configuration options for Consul-based High Availability, aligned with ZooKeeper HA options
 * where applicable (cluster-id, storage path, root path, timeouts).
 */
public final class ConsulHighAvailabilityOptions {

    /** Consul agent host. */
    public static final ConfigOption<String> HA_CONSUL_HOST =
            ConfigOptions.key("high-availability.consul.host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("Consul agent host for HA coordination.");

    /** Consul agent HTTP port. */
    public static final ConfigOption<Integer> HA_CONSUL_PORT =
            ConfigOptions.key("high-availability.consul.port")
                    .intType()
                    .defaultValue(8500)
                    .withDescription("Consul agent HTTP port.");

    /** Root path (prefix) under which Flink stores keys in Consul KV. */
    public static final ConfigOption<String> HA_CONSUL_ROOT =
            ConfigOptions.key("high-availability.consul.path.root")
                    .stringType()
                    .defaultValue("flink")
                    .withDescription("Root path under which Flink stores its entries in Consul KV.");

    /** Path segment for leader latch and leader connection info under root/clusterId. */
    public static final ConfigOption<String> HA_CONSUL_LEADER_PATH =
            ConfigOptions.key("high-availability.consul.path.leader")
                    .stringType()
                    .defaultValue("leader")
                    .withDescription("Path segment for leader election under root/cluster-id.");

    /** Path segment for execution plans (job graphs) under root/clusterId. */
    public static final ConfigOption<String> HA_CONSUL_EXECUTION_PLANS_PATH =
            ConfigOptions.key("high-availability.consul.path.execution-plans")
                    .stringType()
                    .defaultValue("execution-plans")
                    .withDescription("Path segment for execution plans under root/cluster-id.");

    /** Path segment for jobs (checkpoints, etc.) under root/clusterId. */
    public static final ConfigOption<String> HA_CONSUL_JOBS_PATH =
            ConfigOptions.key("high-availability.consul.path.jobs")
                    .stringType()
                    .defaultValue("jobs")
                    .withDescription("Path segment for job-related data under root/cluster-id.");

    /** Token for Consul ACL authentication (token auth only). */
    public static final ConfigOption<String> HA_CONSUL_ACL_TOKEN =
            ConfigOptions.key("high-availability.consul.acl-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Consul ACL token for authentication. Optional if Consul has no ACLs.");

    /** Blocking query wait time for leader retrieval (seconds). */
    public static final ConfigOption<Duration> HA_CONSUL_BLOCKING_QUERY_WAIT =
            ConfigOptions.key("high-availability.consul.blocking-query-wait")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Blocking query wait time for leader retrieval (Consul long polling).");

    /** Session TTL for leader latch (seconds). */
    public static final ConfigOption<Duration> HA_CONSUL_SESSION_TTL =
            ConfigOptions.key("high-availability.consul.session-ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Consul session TTL for leader latch; session is invalidated if not renewed.");

    private ConsulHighAvailabilityOptions() {}
}

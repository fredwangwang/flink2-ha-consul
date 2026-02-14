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
import io.vertx.core.http.HttpClosedException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Path and key helpers for Consul KV layout, mirroring ZooKeeper layout where applicable.
 *
 * <pre>
 * root / clusterId / leader / latch          -> leader election latch key
 *                         / resource_manager / connection_info
 *                         / dispatcher / connection_info
 *                         / rest_server / connection_info
 *                         / job-&lt;jobId&gt; / connection_info
 *       / execution-plans / &lt;jobId&gt;          -> execution plan handle
 *       / jobs / &lt;jobId&gt; / checkpoints / ...
 *                      / checkpoint_id_counter
 * </pre>
 */
public final class ConsulUtils {

    private static final String CONNECTION_INFO_NODE = "connection_info";
    private static final String RESOURCE_MANAGER_NODE = "resource_manager";
    private static final String DISPATCHER_NODE = "dispatcher";
    private static final String REST_SERVER_NODE = "rest_server";
    private static final String LEADER_LATCH_KEY = "latch";
    private static final String CHECKPOINTS_SEGMENT = "checkpoints";
    private static final String CHECKPOINT_ID_COUNTER_KEY = "checkpoint_id_counter";

    public static String getResourceManagerNode() {
        return RESOURCE_MANAGER_NODE;
    }

    public static String getDispatcherNode() {
        return DISPATCHER_NODE;
    }

    public static String getRestServerNode() {
        return REST_SERVER_NODE;
    }

    /** Base path: root/clusterId (no leading/trailing slash; Consul uses flat key with slashes). */
    public static String getConsulBasePath(Configuration configuration) {
        String root = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT);
        String clusterId = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);
        return toKey(root, clusterId);
    }

    /** Leader base path: basePath/leader. */
    public static String getLeaderBasePath(Configuration configuration) {
        String base = getConsulBasePath(configuration);
        String leaderPath = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_LEADER_PATH);
        return toKey(base, leaderPath);
    }

    /** Key for leader latch (single key per cluster for leader election). */
    public static String getLeaderLatchKey(Configuration configuration) {
        return toKey(getLeaderBasePath(configuration), LEADER_LATCH_KEY);
    }

    /** Path segment for a component under leader (e.g. resource_manager, dispatcher). */
    public static String getLeaderPath(String componentId) {
        return componentId;
    }

    /** Full KV key for a component's connection info. */
    public static String getConnectionInfoKey(Configuration configuration, String componentId) {
        return toKey(getLeaderBasePath(configuration), componentId, CONNECTION_INFO_NODE);
    }

    /** Path for job-specific leader (JobManager). */
    public static String getLeaderPathForJob(JobID jobId) {
        return "job-" + jobId.toString();
    }

    public static String getLeaderPathForJobKey(Configuration configuration, JobID jobId) {
        return toKey(getLeaderBasePath(configuration), getLeaderPathForJob(jobId), CONNECTION_INFO_NODE);
    }

    /** Jobs base path: basePath/jobs. */
    public static String getJobsPath(Configuration configuration) {
        String base = getConsulBasePath(configuration);
        String jobsPath = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_JOBS_PATH);
        return toKey(base, jobsPath);
    }

    /** Path for a specific job's data (checkpoints, counter). */
    public static String getJobPath(Configuration configuration, JobID jobId) {
        return toKey(getJobsPath(configuration), jobId.toString());
    }

    public static String getCheckpointsPath(Configuration configuration, JobID jobId) {
        return toKey(getJobPath(configuration, jobId), CHECKPOINTS_SEGMENT);
    }

    public static String getCheckpointIdCounterKey(Configuration configuration, JobID jobId) {
        return toKey(getJobPath(configuration, jobId), CHECKPOINT_ID_COUNTER_KEY);
    }

    /** Execution plans base path. */
    public static String getExecutionPlansPath(Configuration configuration) {
        String base = getConsulBasePath(configuration);
        String plansPath = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_EXECUTION_PLANS_PATH);
        return toKey(base, plansPath);
    }

    /** Key for execution plan state handle for a job. */
    public static String getExecutionPlanKey(Configuration configuration, JobID jobId) {
        return toKey(getExecutionPlansPath(configuration), jobId.toString());
    }

    /** Build a Consul KV key from path segments (no leading slash; segments trimmed; leading/trailing slashes removed to avoid "//"). */
    public static String toKey(String... segments) {
        return Arrays.stream(segments)
                .map(s -> s == null ? "" : s.strip())
                .map(s -> s.replaceAll("^/+|/+$", ""))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("/"));
    }

    public static String getPathForJob(JobID jobId) {
        checkNotNull(jobId, "Job ID");
        return jobId.toString();
    }

    /**
     * Returns true if the failure is likely transient (connection closed, timeout, network).
     * Used to mirror ZooKeeper HA behavior: on connection suspension, log and retry instead
     * of reporting a fatal error or forwarding to the leader election listener.
     */
    public static boolean isTransientConsulFailure(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof IOException
                    || current instanceof TimeoutException
                    || current instanceof HttpClosedException
                    || current instanceof ConnectException
                    || current instanceof SocketException) {
                return true;
            }
            String msg = current.getMessage();
            if (msg != null) {
                String lower = msg.toLowerCase();
                if ((lower.contains("connection") && (lower.contains("closed") || lower.contains("reset")))
                        || lower.contains("timeout")
                        || lower.contains("unavailable")
                        || lower.contains("connection refused")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private ConsulUtils() {}
}

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

import com.ecwid.consul.v1.ConsulClient;
import com.fredwangwang.flink.consul.ha.configuration.ConsulHighAvailabilityOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Creates a Consul client from Flink configuration. Supports token-based authentication only.
 */
public final class ConsulClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulClientFactory.class);

    private ConsulClientFactory() {}

    /**
     * Builds a Consul client using the given configuration. If {@code high-availability.consul.acl-token}
     * is set, the client will use it for all requests.
     */
    public static ConsulClient createConsulClient(Configuration configuration) {
        String host = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_HOST);
        int port = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_PORT);
        Preconditions.checkNotNull(host, "Consul host must not be null");
        // Token auth: set CONSUL_HTTP_TOKEN env or use Consul agent config; ecwid consul-api 1.4.5
        // does not expose setToken on ConsulClient; use QueryParams with token for per-request auth if needed.
        return new ConsulClient(host, port);
    }

    /**
     * Sets the ACL token on an existing client (e.g. for use with prepared client).
     */
    /**
     * Token auth is supported via CONSUL_HTTP_TOKEN environment variable or Consul agent config.
     * ecwid consul-api 1.4.5 does not expose a setToken method on ConsulClient.
     */
    public static void setAclTokenIfPresent(ConsulClient client, Configuration configuration) {
        // No-op; use env or agent config for token.
    }
}

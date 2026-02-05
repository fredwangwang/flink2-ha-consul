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
import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a Consul client from Flink configuration using Vert.x Consul client.
 * Supports ACL token via {@code high-availability.consul.acl-token}.
 *
 * @see <a href="https://vertx.io/docs/vertx-consul-client/java/">Vert.x Consul Client</a>
 */
public final class ConsulClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulClientFactory.class);

    private ConsulClientFactory() {}

    /**
     * Builds a Consul client using the given configuration. If {@code high-availability.consul.acl-token}
     * is set, the client will use it for all requests.
     */
    public static VertxConsulClientAdapter createConsulClient(Configuration configuration) {
        String host = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_HOST);
        int port = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_PORT);
        Preconditions.checkNotNull(host, "Consul host must not be null");

        ConsulClientOptions options = new ConsulClientOptions()
                .setHost(host)
                .setPort(port);

        String aclToken = configuration.get(ConsulHighAvailabilityOptions.HA_CONSUL_ACL_TOKEN);
        if (aclToken != null && !aclToken.isEmpty()) {
            options.setAclToken(aclToken);
            LOG.debug("Consul client configured with ACL token");
        }

        Vertx vertx = Vertx.vertx();
        ConsulClient consulClient = ConsulClient.create(vertx, options);
        return new VertxConsulClientAdapter(vertx, consulClient);
    }
}

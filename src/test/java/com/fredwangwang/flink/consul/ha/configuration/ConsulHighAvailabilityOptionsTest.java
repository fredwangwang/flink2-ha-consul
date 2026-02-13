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

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Unit tests for {@link ConsulHighAvailabilityOptions} defaults and config keys. */
class ConsulHighAvailabilityOptionsTest {

    @Test
    void defaults() {
        Configuration c = new Configuration();
        assertEquals("localhost", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_HOST));
        assertEquals(8500, (int) c.get(ConsulHighAvailabilityOptions.HA_CONSUL_PORT));
        assertEquals("flink", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT));
        assertEquals("leader", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_LEADER_PATH));
        assertEquals("execution-plans", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_EXECUTION_PLANS_PATH));
        assertEquals("jobs", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_JOBS_PATH));
        assertEquals(Duration.ofSeconds(30), c.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL));
        assertEquals(Duration.ofSeconds(15), c.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_LOCK_DELAY));
        assertEquals(Duration.ofSeconds(30), c.get(ConsulHighAvailabilityOptions.HA_CONSUL_BLOCKING_QUERY_WAIT));
    }

    @Test
    void aclTokenHasNoDefault() {
        Configuration c = new Configuration();
        assertFalse(c.getOptional(ConsulHighAvailabilityOptions.HA_CONSUL_ACL_TOKEN).isPresent());
    }

    @Test
    void customValues() {
        Configuration c = new Configuration();
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_HOST, "127.0.0.1");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_PORT, 8500);
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_ACL_TOKEN, "secret");
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL, Duration.ofSeconds(60));
        c.set(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_LOCK_DELAY, Duration.ofSeconds(20));

        assertEquals("127.0.0.1", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_HOST));
        assertEquals(8500, (int) c.get(ConsulHighAvailabilityOptions.HA_CONSUL_PORT));
        assertEquals("secret", c.get(ConsulHighAvailabilityOptions.HA_CONSUL_ACL_TOKEN));
        assertEquals(Duration.ofSeconds(60), c.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL));
        assertEquals(Duration.ofSeconds(20), c.get(ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_LOCK_DELAY));
    }
}

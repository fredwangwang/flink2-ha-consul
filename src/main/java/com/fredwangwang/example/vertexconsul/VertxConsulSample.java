package com.fredwangwang.example.vertexconsul;

import com.fredwangwang.flink.consul.ha.VertxConsulClientAdapter;
import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;

/**
 * Sample that uses VertxConsulClientAdapter to create a Consul session and renew it.
 * Assumes Consul is running on localhost:8500 (default).
 */
public class VertxConsulSample {

    private static final String PROP_HOST = "consul.host";
    private static final String PROP_PORT = "consul.port";
    private static final String PROP_ACL_TOKEN = "consul.aclToken";

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        String host = System.getProperty(PROP_HOST, "localhost");
        int port = Integer.parseInt(System.getProperty(PROP_PORT, "8500"));
        String aclToken = System.getProperty(PROP_ACL_TOKEN, "").trim();

        ConsulClientOptions options = new ConsulClientOptions()
                .setHost(host)
                .setPort(port);
        if (!aclToken.isEmpty()) {
            options.setAclToken(aclToken);
        }
        ConsulClient consulClient = ConsulClient.create(vertx, options);
        VertxConsulClientAdapter adapter = new VertxConsulClientAdapter(vertx, consulClient);

        try {
            // Create a session with 30s TTL
            String sessionId = adapter.sessionCreate("vertx-consul-sample", 30);
            System.out.println("Created session: " + sessionId);

            // Renew the session
            adapter.renewSession(sessionId);
            System.out.println("Renewed session successfully");

            // Clean up: destroy session
            adapter.sessionDestroy(sessionId);
            System.out.println("Destroyed session");

             // Renew the session
             adapter.renewSession(sessionId);
        } finally {
            adapter.close();
        }
    }
}

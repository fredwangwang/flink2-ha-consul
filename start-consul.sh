#!/bin/bash
docker run -d --name consul-server -p 8500:8500 -e 'CONSUL_LOCAL_CONFIG={"acl": {"enabled": true, "default_policy": "deny"}}' hashicorp/consul agent -server -bootstrap -client=0.0.0.0 -ui

docker exec consul-server consul acl bootstrap
#!/bin/bash

mvn package -P flink-2.0,release
mvn package -P flink-2.1,release -DskipTests=true
mvn package -P flink-2.2,release -DskipTests=true

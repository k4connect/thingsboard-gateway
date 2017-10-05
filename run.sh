#!/bin/bash
set -e

mvn compile
mvn exec:java -Dexec.mainClass="org.thingsboard.gateway.GatewayApplication" \
	-DGATEWAY_HOST="localhost" \
	-DGATEWAY_ACCESS_TOKEN="tB1nPdvJBeCwWUmS10qk" \
	-Dlogging.level.org.springframework.boot.autoconfigure.logging=ERROR

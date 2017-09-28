mvn compile
mvn exec:java -Dexec.mainClass="org.thingsboard.gateway.GatewayApplication" \
	-DGATEWAY_HOST="localhost" \
	-DGATEWAY_ACCESS_TOKEN="ixIqRBX2KoAEId6yDpIl" \
	-Dlogging.level.org.springframework.boot.autoconfigure.logging=ERROR

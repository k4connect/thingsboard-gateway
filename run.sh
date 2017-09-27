sed -i '' 's/true/false/' target/classes/tb-gateway.yml
mvn exec:java -Dexec.mainClass="org.thingsboard.gateway.GatewayApplication" -DGATEWAY_HOST="localhost" -DGATEWAY_ACCESS_TOKEN="ixIqRBX2KoAEId6yDpIl"

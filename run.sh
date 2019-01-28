#!/bin/bash
set -e

echo "Preparing environment..."
instanceName=$(aws ec2 describe-tags --filters "Name=resource-id,Values=`ec2metadata --instance-id`" --region us-east-1 | jq '.Tags[] | select (.Key == "Name") | .Value' | tr -d '"')
script="$instanceName.sh"
aws s3 cp s3://k4connect-common-settings/$script ./$script
chmod +x $script
source $script

echo "Compiling thingsboard..."
mvn compile

echo "Running app..."
mvn exec:java -Dexec.mainClass="org.thingsboard.gateway.GatewayApplication" -Dlogging.level.org.springframework.boot.autoconfigure.logging=ERROR

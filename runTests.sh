#!/bin/sh

#
# Builds Thingsboard Gateway and runs the tests with JaCoCo code coverage
# enabled.
#
# The JaCoCo report can be accessed at ./target/site/jacoco/index.html
#

mvn clean jacoco:prepare-agent install jacoco:report

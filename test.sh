#!/bin/sh

# this could be conditionally set.  it may be set higher up as part of the global env variables.
rm -rf target/
# runs in debug mode.
mvn package -Dmaven.surefire.debug 



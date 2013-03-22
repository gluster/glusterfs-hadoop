#!/bin/sh
rm -rf ./target
rm -rf /tmp/gluster/mount/*
mvn package

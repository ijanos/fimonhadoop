#!/bin/bash

hadoop jar fim.jar automation.Automator config.csv

curl --user USER:PASSWORD -X PUT -H "Content-Type: application/json" -d '{ "HostRoles": { "state" : "INSTALLED" } }' node01:8080/api/v1/clusters/TestCluster1/hosts/node03.cluster.kmdm.org/host_components/TASKTRACKER
sleep 30

hadoop jar fim.jar automation.Automator config.csv



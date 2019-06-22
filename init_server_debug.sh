#!/bin/bash

ssh -n $1 nohup java  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=$5 -jar ~/cloud_databases/ms3-server.jar $2 $3 $4 &


#!/bin/bash

ssh -n $1 nohup java -jar ~/cloud_databases/ms3-server.jar $2 $3 $4 &

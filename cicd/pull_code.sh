#!/bin/bash

bash_servers=(
    "spark-00"
    "spark-09"
)

for bash_server in "${bash_servers[@]}"
do
    cmd="ssh root@$bash_server \"sh /opt/itc_process/cicd/pull-code.sh\""
    echo $cmd
    eval $cmd
done


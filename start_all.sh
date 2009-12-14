#!/bin/bash

for host in $*
do
  ssh $host ~/workspace/dqueue/start_data_node.sh
done


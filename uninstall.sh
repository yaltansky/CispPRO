#!/bin/bash

docker rm $(docker stop $(docker ps -q))
docker rmi -f $(docker images -aq)

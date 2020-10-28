sh ./purge-containers.sh ; docker image ls -a | awk '{print $3}' | xargs sudo docker rmi $(sudo docker images -aq) --force

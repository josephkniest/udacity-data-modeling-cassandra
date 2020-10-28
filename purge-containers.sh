docker container ls -a | awk '{print $1}' | xargs sudo docker rm -f

docker run --name sparkify-container -d cassandra
docker exec -it sparkify-container apt-get update
docker exec -it sparkify-container apt-get install -y python3.7
docker exec -it sparkify-container apt-get install -y python3-pip
docker exec -it sparkify-container apt-get install -y libpq-dev
docker exec -it sparkify-container apt-get install -y nano
docker exec -it sparkify-container pip3 install cassandra-driver


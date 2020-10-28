# Image

FROM cassandra

# CPython and udacity dependencies installation

RUN apt-get update

RUN apt-get install -y sudo

RUN apt-get install -y python3.7

RUN apt-get install -y python3-pip

RUN apt-get install -y libpq-dev

RUN apt-get install -y nano

RUN pip3 install cassandra-driver

# Drop udacity project files into image

COPY udacity/ /root

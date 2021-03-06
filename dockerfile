# Image

FROM cassandra

# CPython and GNU installation

RUN apt-get update

RUN apt-get install -y sudo

RUN apt-get install -y python3.7

RUN apt-get install -y python3-pip

RUN apt-get install -y libpq-dev

RUN apt-get install -y nano

RUN pip3 install cassandra-driver

RUN pip3 install autopep8

# Drop udacity project files into image

RUN mkdir /root/data

COPY data/ /root/data

COPY *.py /root

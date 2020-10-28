# Image

FROM cassandra

# CPython and udacity dependencies installation

RUN apt-get update

RUN apt-get install -y sudo

# Drop udacity project files into image

COPY udacity/ /root

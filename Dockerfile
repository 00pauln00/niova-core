FROM ubuntu:18.04

RUN apt-get update -y \
    && apt-get -y install libaio-dev openssl libk5crypto3 librocksdb5.8 uuid vim python3-pip \
    && pip3 install func_timeout sockets psutil dpath \
    && pip3 install ansible \
    && pip3 install jmespath
WORKDIR /opt
COPY . /opt

RUN ls -l /opt

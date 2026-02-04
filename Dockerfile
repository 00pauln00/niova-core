FROM ubuntu:18.04

RUN apt-get update -y \
    && apt-get -y install libaio-dev openssl libk5crypto3 uuid vim python3-pip \
    && pip3 install func_timeout sockets psutil dpath \
    && pip3 install ansible \
    && pip3 install jmespath
WORKDIR /opt

RUN mkdir -p /usr/local/niova/lib \
    && mkdir -p /usr/local/niova/libexec \
    && mkdir -p /usr/local/holon/

COPY holon /usr/local/holon

COPY lib /usr/local/niova/lib
COPY libexec /usr/local/niova/libexec

ENV ANSIBLE_LOOKUP_PLUGINS=/usr/local/holon/
ENV PYTHONPATH=/usr/local/holon/
ENV NIOVA_BIN_PATH=/usr/local/niova/libexec/niova

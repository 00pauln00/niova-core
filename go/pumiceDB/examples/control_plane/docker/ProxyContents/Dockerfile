FROM ubuntu:18.04

#Create dir for niovakv_server and pmdb_server
RUN apt-get update -y \
    && apt-get -y install librocksdb5.8 \
    && mkdir /usr/niovakv_server \
    && mkdir /opt/niova \
    && mkdir /opt/niova/lib

#copy libs,configs and executables
COPY proxy /usr/niovakv_server/proxy
COPY niovakv.config /usr/niovakv_server/config
COPY gossipNodes /usr/niovakv_server/gossipNodes
COPY proxyStarter.sh /usr/starter.sh
ADD  lib /opt/niova/lib

#Set environmental variable
ENV NIOVA_LOCAL_CTL_SVC_DIR="/usr/niovakv_server/PMDBConfig"
ENV LD_LIBRARY_PATH="/opt/niova/lib"

#Run pmdb server
WORKDIR /usr/
RUN chmod +x starter.sh
CMD ./starter.sh

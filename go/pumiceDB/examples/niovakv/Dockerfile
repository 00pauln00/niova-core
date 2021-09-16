FROM ubuntu:18.04

#Create dir for niovakv_server and pmdb_server
RUN apt-get update -y \
    && apt-get -y install librocksdb5.8
RUN mkdir /usr/niovakv_server 
RUN mkdir /usr/pmdb_server 
RUN mkdir /opt/niova
RUN mkdir /opt/niova/lib

#copy libs,configs and executables
COPY niovakv_server/niovakv_server /usr/niovakv_server
COPY niovakv_server/config /usr/niovakv_server
COPY pmdb/server/niovakv_pmdbserver /usr/pmdb_server
COPY docker/starter.sh /usr/
ADD  docker/raftconfig /usr/pmdb_server/raftconfig
ADD  docker/lib /opt/niova/lib

#Set environmental variable
ENV NIOVA_LOCAL_CTL_SVC_DIR="/usr/pmdb_server/raftconfig/configs"
ENV LD_LIBRARY_PATH="/opt/niova/lib"

#Run pmdb server
WORKDIR /usr/
RUN chmod +x starter.sh
CMD ./starter.sh
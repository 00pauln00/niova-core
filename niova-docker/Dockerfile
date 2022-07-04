FROM ubuntu:latest
 
ARG token
ARG recipe_name
ARG backend_type
ARG app_name

# Install golang
RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y git \
  && apt-get install -y autoconf \
  && apt-get install -y libtool \
  && apt-get install -y pkg-config \
  && rm -rf /var/lib/apt/lists/*


RUN wget https://golang.org/dl/go1.18.3.linux-amd64.tar.gz \
    && tar -xzf go1.18.3.linux-amd64.tar.gz \
    && export PATH=$PATH:/go/bin \
    && go version

ENV export PATH=$PATH:/go/bin

# dpkg / rpm prep
RUN apt-get update -y \
    && apt-get install -y uuid-dev uuid libuuid1 libaio-dev libaio1 openssl libssl-dev python3-pip

# Install rocksdb
RUN apt-get install librocksdb-dev -y

# Install dependencies
RUN pip3 install setuptools wheel func_timeout sockets psutil dpath jmespath

# Install ansible
RUN pip3 install ansible

# Copy niova-core
RUN git clone https://$token@github.com/00pauln00/niova-core.git

# Compile niova-core
RUN cd niova-core && ./prepare.sh && ./configure --prefix=/niovacorelibs/ --enable-devel && make clean && make && make install

RUN cd niovacorelibs && mkdir raftconfig

ENV CGO_LDFLAGS="-L/niovacorelibs/lib"
ENV CGO_CFLAGS="-I/niovacorelibs/include/niova"
ENV LD_LIBRARY_PATH=/niovacorelibs/lib
ENV PYTHONPATH=.
ENV NIOVA_BIN_PATH=/niovacorelibs/libexec/niova

# Compile controlplane app
RUN export PATH=$PATH:/go/bin && cd niova-core/go/controlPlane && make -e DIR=/niovacorelibs install_all

# Compile golang apps
#RUN export PATH=$PATH:/go/bin && cd niova-core/go/pumiceDB/examples/niovaKV && make -e DIR=/niovacorelibs niovakv_all

# Clone holon
RUN git clone https://$token@github.com/00pauln00/holon.git

RUN pip3 install generic

# Dsable cache for next tasks
ARG CACHEBUST=1

# Run holon recipe
RUN cd holon && ansible-playbook -e srv_port=8000 -e npeers=5 -e dir_path=/logs/covid_log -e client_port=18000  -e backend_type=$backend_type  -e recipe=$recipe_name.yml -e app_name=$app_name holon.yml

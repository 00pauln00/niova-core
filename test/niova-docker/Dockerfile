FROM nreddy156/niova-base:v2

ARG token
ARG commitID
ARG desc
ARG commit-msg

LABEL description=$desc
LABEL commitID=$commitID
LABEL commit-message=$commit-msg

RUN git config --global url."https://$token@github.com/".insteadOf "https://github.com/"

## Clone repos
# Copy niova-core
RUN git clone https://$token@github.com/00pauln00/niova-core.git && cd niova-core && git checkout pmdb_lookout

# Copy niova-block
RUN git clone https://$token@github.com/00pauln00/niova-block.git
COPY .gitmodules /niova-block

## Compile repos
# Compile niova-core
RUN cd niova-core && ./prepare.sh && ./configure --prefix=/niovacorelibs/ --enable-devel && make clean && make && make install
RUN cd niovacorelibs && mkdir raftconfig

# Set env variables
ENV CGO_LDFLAGS="-L/niovacorelibs/lib"
ENV CGO_CFLAGS="-I/niovacorelibs/include/niova"
ENV LD_LIBRARY_PATH=/niovacorelibs/lib
ENV PYTHONPATH=.
ENV NIOVA_BIN_PATH=/niovacorelibs/libexec/niova


# Compile niova-block
# increase memlock limit
RUN apt-get install -y libreadline-dev
RUN echo "/usr/local/lib" > /tmp/usr-local-lib.conf && cp /tmp/usr-local-lib.conf /etc/ld.so.conf.d && ldconfig
RUN echo '* hard memlock 65536' | tee -a /etc/security/limits.conf
RUN cd niova-block && git submodule init && git submodule update
RUN cd niova-block/modules/liburing && ./configure && make && make install
RUN cd niova-block && ./prepare.sh && ./configure --with-niova=/niovacorelibs/ --prefix=/niova-block/ --enable-devel && make clean && make && make install
RUN cp niova-block/bin/niova-block-ctl /niovacorelibs && cp niova-block/bin/nisd /niovacorelibs

# Compile niova-lookout
RUN export PATH=$PATH:/go/bin && cd niova-core/go/lookout/nisdMonitor/ && go build && cp nisdLookout /niovacorelibs/libexec/niova

# Compile controlplane app
RUN export PATH=$PATH:/go/bin && cd niova-core/go/controlPlane && make -e DIR=/niovacorelibs install_all

# Compile golang apps
RUN export PATH=$PATH:/go/bin && cd niova-core/go/pumiceDB/examples/niovaKV && make -e DIR=/niovacorelibs niovakv_all

# Clone holon
RUN git clone https://$token@github.com/00pauln00/holon.git && cd holon && git checkout prometheus_support

# Dsable cache for next tasks
ARG CACHEBUST=1

# Install prometheus client
RUN wget https://github.com/prometheus/prometheus/releases/download/v2.38.0/prometheus-2.38.0.linux-amd64.tar.gz
RUN mkdir prometheus && tar -xvf prometheus-2.38.0.linux-amd64.tar.gz -C holon --strip-components=1
COPY prometheus.yml /holon
COPY nodeParser.py /holon

# Run holon recipe
WORKDIR /holon
COPY entrypoint.sh /holon

ENTRYPOINT ["bash", "entrypoint.sh"]

#ENTRYPOINT ["ansible-playbook", "-e", "srv_port=2530", "-e", "dir_path=/logs/covid_log", "-e", "client_port=18000","holon.yml"] 


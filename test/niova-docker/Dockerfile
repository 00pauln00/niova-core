FROM nreddy156/niova-base:v2

ARG token
ARG commitID
ARG desc
ARG commit-msg

LABEL description=$desc
LABEL commitID=$commitID
LABEL commit-message=$commit-msg

RUN git config --global url."https://$token@github.com/".insteadOf "https://github.com/"

# Copy niova-core
RUN git clone https://$token@github.com/00pauln00/niova-core.git

# Copy niova-block
RUN git clone https://$token@github.com/00pauln00/niova-block.git
COPY .gitmodules /niova-block

# Copy niova-lookout
RUN git clone https://$token@github.com/00pauln00/niova-lookout.git

# Compile niova-core
RUN cd niova-core && ./prepare.sh && ./configure --prefix=/niovacorelibs/ --enable-devel && make clean && make && make install
RUN cd niovacorelibs && mkdir raftconfig

ENV CGO_LDFLAGS="-L/niovacorelibs/lib"
ENV CGO_CFLAGS="-I/niovacorelibs/include/niova"
ENV LD_LIBRARY_PATH=/niovacorelibs/lib
ENV PYTHONPATH=.
ENV NIOVA_BIN_PATH=/niovacorelibs/libexec/niova

# Compile niova-lookout
RUN export PATH=$PATH:/go/bin && cd niova-lookout && go build && cp lookout /niovacorelibs/libexec/niova

# Compile niova-block
# increase memlock limit
RUN apt-get install -y libreadline-dev
RUN echo "/usr/local/lib" > /tmp/usr-local-lib.conf && cp /tmp/usr-local-lib.conf /etc/ld.so.conf.d && ldconfig
RUN echo '* hard memlock 65536' | tee -a /etc/security/limits.conf
RUN cd niova-block && git submodule init && git submodule update 
RUN cd niova-block/modules/liburing && ./configure && make && make install
RUN cd niova-block && ./prepare.sh && ./configure --with-niova=/niovacorelibs/ --prefix=/niova-block/ --enable-devel && make clean && make && make install 
RUN cp niova-block/bin/niova-block-ctl /niovacorelibs && cp niova-block/bin/nisd /niovacorelibs

# Compile controlplane app
RUN export PATH=$PATH:/go/bin && cd niova-core/go/controlPlane && make -e DIR=/niovacorelibs install_all

# Compile golang apps
RUN export PATH=$PATH:/go/bin && cd niova-core && git checkout permission-lvl && cd go/pumiceDB/examples/niovaKV && make -e DIR=/niovacorelibs niovakv_all

# Clone holon
RUN git clone https://$token@github.com/00pauln00/holon.git

# Dsable cache for next tasks
ARG CACHEBUST=1

# Run holon recipe
WORKDIR /holon
ENTRYPOINT ["ansible-playbook", "-e", "srv_port=8000", "-e", "dir_path=/logs/covid_log", "-e", "client_port=18000","holon.yml"]

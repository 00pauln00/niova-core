FROM nreddy156/niova-base:v1

ARG token
 
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
WORKDIR /holon
ENTRYPOINT ["ansible-playbook", "-e", "srv_port=8000", "-e", "npeers=5", "-e", "dir_path=/logs/covid_log", "-e", "client_port=18000","holon.yml"]

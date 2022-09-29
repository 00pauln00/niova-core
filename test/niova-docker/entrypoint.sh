#!bin/bash

# OS Distro
temp="uname -o"
OS=$(eval "$temp")

# Node name
temp1="uname --nodename"
NODE=$(eval "$temp1")

# Hardware platform name
temp1="uname -i"
HW=$(eval "$temp1")

echo "Running on : $OS"
echo "Node : $NODE"
echo "HW : $HW"

# Run prometheus 
cd /holon
./prometheus --config.file=prometheus.yml &

# Run grafana
service grafana-server start

# Run command passed from arguments
$*

# Run 'cat' so that the Dockerfile does not leave the scope of entrypoint.sh
cat

Steps to run containers<br>
<ul>
<li>1. Run make file in niovakv dir, with arg DIR set to niovacore binaries folder</li>
<li>2. Place a log file from an existing run of holon in above mentioned dir with folder name as "raftconfig" instead of raftuuid.</li>
<li>3. Execute prepare_docker.sh in the niovacore binaries folder</li>
<li>4. Run the ncpc binary with the path to the raftconfig file in the niova binaries folder in order to test.<li>
</ul>
<br>
5 Containers are created, each with pmdb server and a niova_kv server. The log file of the execution are place the Node# dir in this dir.

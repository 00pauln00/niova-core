Steps to run containers<br>
<ul>
<li>1. Set LD_LIBRARY_PATH to the niova-core libs file</li>
<li>2. Place raft config file in current dir with folder name as "raftconfig" instead of raftuuid.</li>
<li>3. Compile niovakv_server and pmdb_server, later will add make file to the script</li>
<li>4. Execute prepare_docker.sh</li>
</ul>
<br>
5 Containers are created, each with pmdb server and a niova_kv server. The log file of the execution are place the Node# dir in this dir.
Steps to run containers<br>
<ul>
<li>1. Run make file in niovakv dir, with arg DIR set to niovacore compiled folder</li>
<li>2. Place raft config folder in above mentioned dir with folder name as "raftconfig" instead of raftuuid.</li>
<li>3. Execute prepare_docker.sh</li>
</ul>
<br>
5 Containers are created, each with pmdb server and a niova_kv server. The log file of the execution are place the Node# dir in this dir.

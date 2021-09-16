Steps to run containers<br>
<ul>
<li>1. Set LD_LIBRARY_PATH to the niova-core libs file</li>
<li>2. Place raft config file in current dir with folder name as "raftconfig". For example a sample raft config file is placed in this dir, please replace it as required</li>
<li>3. Execute prepare_docker.sh</li>
</ul>
<br>
5 Containers are created, each with pmdb server and a niova_kv server. The log file of the execution are place the Node# dir in this dir.
rm -rf Node*
cp -r $LD_LIBRARY_PATH ./
for i in 1 2 3 4 5
do 
    mkdir "Node"$i
    cd "Node"$i
    mkdir niovakv_server_logs
    mkdir pmdb_server_logs
    cd ..
done
cd ..
sudo docker-compose build
sudo docker-compose up
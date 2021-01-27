# Starting via docker compose

$ docker-compose up

Navigate browser to http://localhost:3000

# Building Docker Images

## Webapp

docker build -t niova-config-webapp -f Dockerfile.webapp . 

## Server

docker build -t niova-config-server -f Dockerfile.server . 

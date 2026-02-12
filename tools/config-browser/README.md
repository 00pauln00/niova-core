# Starting via docker compose

$ docker-compose up

Navigate browser to http://localhost:3000

# Building Docker Images

## Webapp

docker build -t niova-config-webapp -f Dockerfile.webapp . 

## Server

docker build -t niova-config-server -f Dockerfile.server . 

# Starting via nodejs

This is made using node 14, later versions probably won't work. nvm allows you to select a node version.

## nvm set up

$ curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
$ nvm install 14
$ bash (or relogin)
$ node -v
v14.21.3

## yarn install

$ npm -i yarn
$ yarn

## Webapp

$ yarn start

## Server

$ yarn server


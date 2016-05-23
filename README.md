TCP proxy for buffering connections to Airbrake and Errbit so that they don't add request latency to reporting applications

## Prerequisites

* [Node.js][node] 0.8.8
* [Redis]

## Install & Setup

Get the code from [GitHub][router]:

    cd ~/Projects
    git clone git@github.com:globaldev/airbrake-proxy.git
    cd airbrake-proxy

Installing the dependancies:

    npm install

If an `CERT_UNTRUSTED` error is thrown during `npm install` try:

    npm config set strict-ssl false

Create configuration files by copying the example configuration:

    cp config/config.json.example config/config.json

## Running

    cd ~/Projects/wld-api-router
    npm start

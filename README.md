TCP proxy for buffering connections to Sentry so that they don't add request latency to reporting applications

## Prerequisites

* [Node.js][node] 0.8.8
* [Redis]

## Install & Setup

Get the code from [GitHub][router]:

    cd ~/Projects
    git clone git@github.com:globaldev/error-proxy.git
    cd error-proxy

Install NodeJS 0.8.8

    brew install n
    n 0.8.8

Installing the dependencies:

    npm install

If an `CERT_UNTRUSTED` error is thrown during `npm install` try:

    npm config set strict-ssl false

Create configuration files by copying the example configuration:

    cp config/config.json.example config/config.json


##Adding a new project

1. Get Ops to create the project in Sentry

2. Update config.json with those details

	```
    "UUID_string": {
        "id": "SENTRY_PROJECT_ID",
        "key": "SENTRY_KEY",
        "secret": "SENTRY_SECRET",
        "platform": "SENTRY_PLATFORM"
    }
	```
	If the project exists in Airbrake set the Airbrake key as the `UUID_string`.

3. Update your app to point to the error proxy and set the api_key to the UUID_string.

	```
	Airbrake.configure do |config|
	  config.api_key = "UUID_string"
	  config.host = "errorproxy1"
	  config.port = "6633"
	end
	```

## Running

    cd ~/Projects/error-proxy
    npm start

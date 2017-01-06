#!/usr/bin/env node

/**
 * server.js
 *
 * This is the master error-proxy process responsible for controlling the
 * main worker cluster that deals with listening to Airbrake client requests
 * and storing them in Sentry.
 */

/*
 * Modules
 */

var util = require('util');
var http = require('http');
var zlib = require('zlib');
var crypto = require('crypto');
var cluster = require('cluster');
var microtime = require('microtime');
var nodestatsd = require('node-statsd').StatsD;
var xmlparse = require('xml2js').parseString;
var uuid = require('./lib/uuid');

/*
 * Configuration
 */

// Load configuration from disk
try {
	var configjson = require('./config/config.json');
} catch (e) {
	util.log("Could not load configuration, did you create config.json?");
	process.exit(1);
}

// Parse configuration against defaults
var config = require('./lib/config').config(configjson);

// Choose protocol to connect to Sentry with
config.sentry.connection = (config.sentry.protocol === "https") ? require('https') : require('http');

// Create Redis connection
var redis = require('redis').createClient(config.redis.port, config.redis.host, {'enable_offline_queue': false});

redis.on('error', function (error) {
	util.log("Could not create a Redis client connection to " + config.redis.host + ":" + config.redis.port);
	process.exit(6);
});

// Create StatsD connection
var statsd = new nodestatsd(config.statsd.host, config.statsd.port);

// Airbrake style XML response to send to client
var xmlresponse = '<?xml version="1.0"?><notice><id>{UUID}</id><url>http://' + config.listen.hostname + ':' + config.listen.port + '/locate/{UUID}</url></notice>';

/*
 * Worker cluster
 */

if (cluster.isMaster) {
	// Give the master a proper process title in the process list
	process.title = 'error-proxy';

	// Helper function to fork a child worker
	var fork = function () {
		cluster.fork();
	};

	// Create enough workers to satisfy passed worker count
	for (var child = 0; child < config.listen.workers; child++) {
		fork();
	}

	// If a cluster worker exits, create a new one
	cluster.on('exit', function (worker, code, signal) {
		fork();
	});
} else if (cluster.isWorker) {
	util.log('Child ' + cluster.worker.process.pid + ' started, listening to client requests');

	// Create and make a request to Sentry
	var storeSentry = function (responseuuid, data) {
		xmlparse(data, function(error, xml) {
			if (typeof(config.sentry.projects[xml.notice['api-key']]) == "undefined") {
				util.log("Airbrake API key '" + xml.notice['api-key'] + "' is not defined in Sentry projects configuration, will not send to Sentry");
				return;
			}

			hostname = (typeof(xml.notice['server-environment'][0].hostname) == "undefined") ? "None" : xml.notice['server-environment'][0].hostname[0];

			// Backtrace frames
			var frames = [];
			var lines = xml.notice.error[0].backtrace[0].line;

			// Make sure the final frame has usable details
			var finalLine = {$: ''};
			while (! finalLine.$.file || ! finalLine.$.number) {
				finalLine = lines.pop();
			}

			lines.forEach(function(line) {
				if (line.$.file && line.$.number) {
					frames.push({
						"filename": line.$.file.replace('[PROJECT_ROOT]', xml.notice['server-environment'][0]['project-root'][0]),
						"lineno": line.$.number,
						"function": line.$.method,
						"in_app": true,
						"module": "node"
					});
				}
			});

			frames.push({
				"filename": finalLine.$.file.replace('[PROJECT_ROOT]', xml.notice['server-environment'][0]['project-root'][0]),
				"lineno": finalLine.$.number,
				"function": finalLine.$.method,
				"in_app": true,
				"module": "exception"
			});

			// Environment variables for request object
			var env = {};

			if (typeof(xml.notice.request) != "undefined" && typeof(xml.notice.request[0]['cgi-data']) != "undefined") {
				xml.notice.request[0]['cgi-data'][0].var.forEach(function(data) {
					env[data.$.key] = (typeof(data._) == "undefined") ? "" : data._.toString();
				});
			}

			// Parameter data for request object
			var params = {};
			if (typeof(xml.notice.request) != "undefined" && typeof(xml.notice.request[0]['params']) != "undefined") {
				xml.notice.request[0]['params'][0].var.forEach(function(data) {
					params[data.$.key] = (typeof(data._) == "undefined") ? "" : data._.toString();
				});
			}

			// Sentry JSON object
			var sentry = {
				"message": xml.notice.error[0].message[0],
					"exception": {
					"type": xml.notice.error[0].class[0],
					"value": xml.notice.error[0].message[0]
				},
				"stacktrace": {
					"frames": frames
				},
				"culprit": xml.notice.error[0].message[0],
				"server_name": hostname,
				"extra": {},
				"logger": "",
				"timestamp": Date.now(),
				"project": config.sentry.projects[xml.notice['api-key']].id,
				"platform": config.sentry.projects[xml.notice['api-key']].platform,
				"environment": xml.notice['server-environment'][0]['environment-name'][0]
			};

			if (typeof(xml.notice.request) != "undefined") {
				sentry["request"] = {
					"url": xml.notice.request[0].url[0],
					"data": params,
					"env": env
				};
			}

			sentry['event_id'] = crypto.createHash('md5').update(JSON.stringify(sentry)).digest('hex');

			// GZ compress
			zlib.deflate(JSON.stringify(sentry), function(error, gz) {
				if (error) {
					util.log("Error compressing Sentry object: " + error);
				} else {
					// Encode to base64
					var base64 = new Buffer(gz).toString('base64');

					// POST to Sentry
					var sentryRequestOptions = {
						host: config.sentry.host,
						port: config.sentry.port,
						path: '/api/' + config.sentry.projects[xml.notice['api-key']].id + '/store/',
						method: 'POST',
						headers: {
							'Connection': 'close',
							'Content-Type': 'application/octet-stream',
							'Content-Length': base64.length,
							'X-Sentry-Auth': 'Sentry sentry_version=5, sentry_timestamp=' + Date.now() + '000, sentry_client=error-proxy/0.1.0, sentry_key=' + config.sentry.projects[xml.notice['api-key']].key +', sentry_secret=' + config.sentry.projects[xml.notice['api-key']].secret + ''
						}
					};

					var startSentry = microtime.now();

					// Make the request to Sentry
					var sentryRequest = config.sentry.connection.request(sentryRequestOptions, function(sentryResult) {
						var responseData = '';
						sentryResult.on('data', function (chunk) {
							responseData += chunk;
						}).on('end', function () {
							var endSentry = microtime.now();
							statsd.timing(config.statsd.prefix + '.sentry.request', ((endSentry - startSentry) / 1000));
							try {
								var sentry = JSON.parse(responseData);
								if (sentry.id) {
									redis.hset(config.redis.key, responseuuid, sentry.id);
									statsd.increment(config.statsd.prefix + '.sentry.request.success');
								} else {
									throw new Error("JSON response from Sentry contained no success ID");
								}
							} catch (responseError) {
								statsd.increment(config.statsd.prefix + '.sentry.request.fail.json');
								util.log("JSON response returned from " + config.sentry.host + ":" + config.sentry.port + " is invalid (" + responseError + "); response: " + responseData);
							}
						});
					});

					// Set a connection timeout
					sentryRequest.on('socket', function (socket) {
						socket.setTimeout(config.sentry.timeout);
						socket.on('timeout', function () {
							statsd.increment(config.statsd.prefix + '.sentry.request.fail.timeout');
							util.log("Connection to " + config.sentry.host + ":" + config.sentry.port + " for " + responseuuid + " timed out after " + config.sentry.timeout + "ms");
							sentryRequest.abort();
						});
					});

					// Drop the request if we had a communication error
					sentryRequest.on('error', function (error) {
						util.log("Failed sending request to " + config.sentry.host + ":" + config.sentry.port + " for " + responseuuid + ", exception lost; error: " + error);
						sentryRequest.abort();
						statsd.increment(config.statsd.prefix + '.sentry.request.fail.error');
					});

					// Send the Sentry object data and end the connection
					sentryRequest.write(base64);
					sentryRequest.end();
				}
			});
		});
	};

	// Create an HTTP server to listen to lookup GET requests and Airbrake client POST requests
	http.createServer(function (request, response) {
		var requesturl = request.url;
		var startHTTP = microtime.now();

		if (request.method === "GET") {
			requesturl = requesturl.replace(/\/locate\//g, '').replace(/\//g, '').substring(0, 36);
			redis.hget(config.redis.key, requesturl, function (error, reply) {
				if (reply) {
					response.writeHead(303, {
						'Location': 'https://airbrake.io/locate/' + reply,
					});
				} else {
					response.writeHead(404);
				}

				response.end();
				request.connection.destroy();
			});
		} else {
			var data = '';
			request.on('data', function (chunk) {
				data += chunk;
			}).on('end', function () {
				// Create a UUID and immediately return it to the client
				var responseuuid = uuid.uuid();
				response.writeHead(200);
				response.end(xmlresponse.replace(/{UUID}/g, responseuuid));
				request.connection.destroy();

				// Store stats about the request
				var endHTTP = microtime.now();
				statsd.timing(config.statsd.prefix + '.http.request', ((endHTTP - startHTTP) / 1000));

				// Store the generated UUID in Redis
				redis.hset(config.redis.key, responseuuid, "null");

				// If Sentry configuration is defined, create and send a Sentry request object to the Sentry host
				if (config.sentry.host != "") {
					storeSentry(responseuuid, data);
				}
			});
		}
	}).listen(config.listen.port, config.listen.host);
}

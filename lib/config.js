exports.config = function (json) {

	// Defaults
	var config = {
		'listen': {
			'host': "127.0.0.1",
			'port': 6633,
			'workers': 1,
			'hostname': "localhost"
		},
		'airbrake': {
			'host': "api.airbrake.io",
			'port': 443,
			'timeout': 10000,
			'protocol': "https"
		},
		'sentry': {
			'host': "",
			'port': 443,
			'timeout': 10000,
			'protocol': "https",
			'projects': {}
		},
		'hosted_sentry': {
			'host': 'app.getsentry.com',
			'port': 443,
			'timeout': 10000,
			'protocol': 'https',
			'projects': []
		},
		'redis': {
			'host': "127.0.0.1",
			'port': 6379,
			'prefix': "airbrakeproxy"
		},
		'statsd': {
			'host': "127.0.0.1",
			'port': 8125,
			'prefix': "airbrakeproxy"
		}
	};

	// Iterate through config object and replace with supplied configuration
	Object.keys(config).forEach(function (topic) {
		if ((typeof(config[topic]) == "object") && json[topic]) {
			Object.keys(config[topic]).forEach(function (key) {
				if (json[topic][key] !== undefined) {
					config[topic][key] = json[topic][key];
				}
			});
		} else {
			if (json[topic] !== undefined) {
				config[topic] = json[topic];
			}
		}
	});

	// Redis keys
	config.redis.key = config.redis.prefix + ":uuid";

	return config;

};

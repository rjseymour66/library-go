package config

import "time"

var (
	// GetHTTPServerAddress returns the server address from
	// the [http] section in the .toml config file
	GetHTTPServerAddress = getHTTPServerAddress

	// GetHTTPReadTimeout returns the read_timeout value from
	// the [http] section in the .toml config file
	GetHTTPReadTimeout = getHTTPReadTimeout

	// GetHTTPReadTimeout returns the write_timeout value from
	// the [http] section in the .toml config file
	GetHTTPWriteTimeout = getHTTPWriteTimeout
)

func getHTTPServerAddress() string {
	return getConfigString("http.server_address")
}

func getHTTPReadTimeout() time.Duration {
	return getConfigDuration("http.read_timeout")
}

func getHTTPWriteTimeout() time.Duration {
	return getConfigDuration("http.write_timeout")
}

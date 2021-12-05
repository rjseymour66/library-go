package config

import (
	"time"

	"github.com/spf13/viper"
)

var (
	// InitConfig reads the configuration from the TOML file
	InitConfig = initConfig
)

// initConfig reads the configuration file while the program is running.
func initConfig(fileName string, addtionalDirs []string) (err error) {
	// Pass the config file
	viper.SetConfigName(fileName)
	// Tell viper to search in current and $HOME dirs
	// for config files
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME")

	// Add any additional config files to the loop
	for _, dir := range addtionalDirs {
		viper.AddConfigPath(dir)
	}

	// Read configuration file from disk
	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	// Return the configuration file
	viper.ConfigFileUsed()
	// Watch the config for changes
	viper.WatchConfig()

	// return err because if execution gets this far, the
	// initConfig function was successful
	return err
}

// The following functions return config values that
// are useful if you want to override specific settings
// for a custom config.
func getConfigString(key string) string {
	return viper.GetString(key)
}

func getConfigInt(key string) int {
	return viper.GetInt(key)
}

func getConfigDuration(key string) time.Duration {
	return viper.GetDuration(key)
}

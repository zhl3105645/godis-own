package main

import (
	"fmt"
	"godis/config"
	"godis/lib/logger"
	"godis/lib/utils"
	RedisServer "godis/redis/server"
	"godis/tcp"
	"os"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`
var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
}

func main() {
	print(banner)
	logger.Setup(&logger.Setting{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2001-01-01",
	})
	configName := os.Getenv("CONFIG")
	if configName == "" {
		if utils.FileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configName)
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%s", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}

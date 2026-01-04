package common

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID       string `yaml:"node_id"`
	Address      string `yaml:"address"`
	MetadataAddr string `yaml:"metadata_addr"`
	DataDir      string `yaml:"data_dir"`
}

func LoadConfig(path string) Config {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var c Config

	if err := yaml.Unmarshal(b, &c); err != nil {
		panic(err)
	}

	return c
}

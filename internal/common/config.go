package common

import (
	"os"

	"gopkg.in/yaml.v3"
)

// MetadataConfig matches config/metadata.yaml structure
type MetadataConfig struct {
	Address           string `yaml:"address"`
	ReplicationFactor int    `yaml:"replication_factor"`
	Heartbeat         struct {
		TTLSeconds             int `yaml:"ttl_seconds"`
		CleanupIntervalSeconds int `yaml:"cleanup_interval_seconds"`
	} `yaml:"heartbeat"`
	WAL struct {
		Path string `yaml:"path"`
	} `yaml:"wal"`
	Snapshot struct {
		Path            string `yaml:"path"`
		IntervalSeconds int    `yaml:"interval_seconds"`
	} `yaml:"snapshot"`
}

// DataNodeConfig matches config/datanode.yaml structure
type DataNodeConfig struct {
	NodeID          string `yaml:"node_id"`
	Address         string `yaml:"address"`
	DataDir         string `yaml:"data_dir"`
	MetadataAddress string `yaml:"metadata_address"`
	Heartbeat       struct {
		IntervalSeconds int `yaml:"interval_seconds"`
	} `yaml:"heartbeat"`
	GRPC struct {
		MaxMsgMB int `yaml:"max_msg_mb"`
	} `yaml:"grpc"`
}

// ClientConfig matches config/client.yaml structure
type ClientConfig struct {
	MetadataAddress string `yaml:"metadata_address"`
	Timeouts        struct {
		RPCSeconds int `yaml:"rpc_seconds"`
	} `yaml:"timeouts"`
	Concurrency struct {
		UploadWorkers int `yaml:"upload_workers"`
	} `yaml:"concurrency"`
}

// LoadMetadataConfig loads metadata server configuration
func LoadMetadataConfig(path string) (MetadataConfig, error) {
	var cfg MetadataConfig

	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// LoadDataNodeConfig loads datanode configuration
func LoadDataNodeConfig(path string) (DataNodeConfig, error) {
	var cfg DataNodeConfig

	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// LoadClientConfig loads client configuration
func LoadClientConfig(path string) (ClientConfig, error) {
	var cfg ClientConfig

	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

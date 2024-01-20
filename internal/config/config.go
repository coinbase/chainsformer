package config

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	"github.com/coinbase/chainstorage/sdk"

	"github.com/coinbase/chainsformer/config"
	"github.com/coinbase/chainsformer/internal/utils/auxiliary"
)

type (
	Config struct {
		ConfigName      string                `mapstructure:"config_name" validate:"required"`
		Chain           ChainConfig           `mapstructure:"chain"`
		SLA             SLAConfig             `mapstructure:"sla"`
		Table           TableConfig           `mapstructure:"table" validate:"required"`
		Server          ServerConfig          `mapstructure:"server"`
		ChainStorageSDK ChainStorageSDKConfig `mapstructure:"chainstorage_sdk" validate:"required"`
		StatsD          *StatsDConfig         `mapstructure:"statsd"`

		env Env
	}

	ChainConfig struct {
		Blockchain common.Blockchain `mapstructure:"blockchain" validate:"required"`
		Network    common.Network    `mapstructure:"network" validate:"required"`
	}

	SLAConfig struct {
		Tier int `mapstructure:"tier" validate:"required"` // 1 for high urgency; 2 for low urgency; 3 for work in progress.
	}

	TableConfig struct {
		SupportedFormats []string          `mapstructure:"supported_formats" validate:"required"`
		StreamTable      StreamTableConfig `mapstructure:"stream_table"`
	}

	StreamTableConfig struct {
		Parallelism int `mapstructure:"parallelism"`
	}

	ServerConfig struct {
		BindAddress string `mapstructure:"bind_address" validate:"required"`
	}

	ChainStorageSDKConfig struct {
		sdk.Config `mapstructure:",squash"`
	}

	StatsDConfig struct {
		Address string `mapstructure:"address" validate:"required"`
		Prefix  string `mapstructure:"prefix"`
	}

	Env string

	AWSAccount string

	ConfigOption func(options *configOptions)

	configOptions struct {
		Blockchain common.Blockchain `validate:"required"`
		Network    common.Network    `validate:"required"`
		Env        Env               `validate:"required,oneof=data-shared-prod data-shared-dev local"`
	}

	// derivedConfig defines a callback where a config struct can override its fields based on the global config.
	// For example, ChainStorageSDKConfig implements this interface to copy the global tag into its own struct.
	derivedConfig interface {
		DeriveConfig(cfg *Config)
	}
)

const (
	EnvVarConfigName  = "CHAINSFORMER_CONFIG"
	EnvVarEnvironment = "CHAINSFORMER_ENVIRONMENT"
	EnvVarConfigRoot  = "CHAINSFORMER_CONFIG_ROOT"
	EnvVarConfigPath  = "CHAINSFORMER_CONFIG_PATH"
	EnvVarTestType    = "TEST_TYPE"
	EnvVarCI          = "CI"

	Namespace         = "chainsformer"
	DefaultConfigName = "ethereum-mainnet"

	EnvBase        Env = "base"
	EnvLocal       Env = "local"
	EnvDevelopment Env = "development"
	EnvProduction  Env = "production"

	AWSAccountDevelopment AWSAccount = "development"
	AWSAccountProduction  AWSAccount = "production"

	tagBlockchain = "blockchain"
	tagNetwork    = "network"
	tagTier       = "tier"

	defaultStreamParallelism = 10
)

var (
	_ derivedConfig = (*ChainStorageSDKConfig)(nil)

	AWSAccountEnvMap = map[AWSAccount]Env{
		"":                    EnvLocal,
		AWSAccountDevelopment: EnvDevelopment,
		AWSAccountProduction:  EnvProduction,
	}
)

func New(opts ...ConfigOption) (*Config, error) {
	configName, ok := os.LookupEnv(EnvVarConfigName)
	if !ok {
		configName = DefaultConfigName
	}

	configOpts, err := getConfigOptions(configName, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get config options %w", err)
	}

	configReader, err := getConfigData(Namespace, EnvBase, configOpts.Blockchain, configOpts.Network)
	if err != nil {
		return nil, xerrors.Errorf("failed to locate config file: %w", err)
	}

	v := viper.New()
	v.SetConfigName(string(EnvBase))
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)
	v.SetEnvPrefix("CHAINSFORMER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	cfg := Config{
		env: configOpts.Env,
	}

	if err := v.ReadConfig(configReader); err != nil {
		return nil, xerrors.Errorf("failed to read config: %w", err)
	}

	if err := mergeInConfig(v, configOpts, configOpts.Env); err != nil {
		return nil, xerrors.Errorf("failed to merge in %v config: %w", configOpts.Env, err)
	}

	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.TextUnmarshallerHookFunc(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		stringToBlockchainHookFunc(),
		stringToNetworkHookFunc(),
	))); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.setDerivedConfigs(reflect.ValueOf(&cfg))

	validate := validator.New()
	if err := validate.Struct(&cfg); err != nil {
		return nil, xerrors.Errorf("failed to validate config: %w", err)
	}

	return &cfg, nil
}

func GetEnv() Env {
	awsAccount := AWSAccount(os.Getenv(EnvVarEnvironment))
	env, ok := AWSAccountEnvMap[awsAccount]
	if !ok {
		return EnvLocal
	}

	return env
}

func GetConfigRoot() string {
	return os.Getenv(EnvVarConfigRoot)
}

func GetConfigPath() string {
	return os.Getenv(EnvVarConfigPath)
}

func getConfigOptions(configName string, opts ...ConfigOption) (*configOptions, error) {
	configOpts := &configOptions{}
	for _, opt := range opts {
		opt(configOpts)
	}

	if configOpts.Env == "" {
		configOpts.Env = GetEnv()
	}

	if configOpts.Blockchain == common.Blockchain_BLOCKCHAIN_UNKNOWN && configOpts.Network == common.Network_NETWORK_UNKNOWN {
		blockchain, network, err := ParseConfigName(configName)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse config name: %w", err)
		}

		configOpts.Blockchain = blockchain
		configOpts.Network = network
	}

	return configOpts, nil
}

func ParseConfigName(configName string) (common.Blockchain, common.Network, error) {
	// Normalize the config name by replacing "-" with "_".
	configName = strings.ReplaceAll(configName, "-", "_")

	splitString := strings.Split(configName, "_")
	if len(splitString) != 2 {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("config name is invalid: %v", configName)
	}

	blockchainName := splitString[0]
	blockchain, err := auxiliary.ParseBlockchain(blockchainName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("failed to parse blockchain from config name %v: %w", configName, err)
	}

	networkName := fmt.Sprintf("%v_%v", splitString[0], splitString[1])
	network, err := auxiliary.ParseNetwork(networkName)
	if err != nil {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN, common.Network_NETWORK_UNKNOWN, xerrors.Errorf("failed to parse network from config name %v: %w", configName, err)
	}

	return blockchain, network, nil
}

func stringToBlockchainHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Blockchain_BLOCKCHAIN_UNKNOWN) {
			return data, nil
		}

		return common.Blockchain_value[data.(string)], nil
	}
}

func stringToNetworkHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(common.Network_NETWORK_UNKNOWN) {
			return data, nil
		}

		return common.Network_value[data.(string)], nil
	}
}

func getConfigData(namespace string, env Env, blockchain common.Blockchain, network common.Network) (io.Reader, error) {
	blockchainName := blockchain.GetName()
	networkName := strings.TrimPrefix(network.GetName(), blockchainName+"-")
	configRoot := GetConfigRoot()
	configPath := GetConfigPath()

	// If configPath is not set, try to construct the file system path from configRoot.
	if len(configPath) == 0 && len(configRoot) > 0 {
		configPath = fmt.Sprintf("%v/%v/%v/%v/%v.yml", configRoot, namespace, blockchainName, networkName, env)
	}

	// If either configRoot or configPath is set, read the config from the file system.
	if len(configPath) > 0 {
		reader, err := os.Open(configPath)
		if err != nil {
			return nil, xerrors.Errorf("failed to read config file %v: %w", configPath, err)
		}
		return reader, nil
	}

	configPath = fmt.Sprintf("%s/%v/%v/%v.yml", namespace, blockchainName, networkName, env)

	return config.ConfigFS.Open(configPath)
}

func mergeInConfig(v *viper.Viper, configOpts *configOptions, env Env) error {
	// Merge in the env-specific config if available.
	if configReader, err := getConfigData(Namespace, env, configOpts.Blockchain, configOpts.Network); err == nil {
		v.SetConfigName(string(env))
		if err := v.MergeConfig(configReader); err != nil {
			return xerrors.Errorf("failed to merge config %v: %w", configOpts.Env, err)
		}
	}
	return nil
}

func WithBlockchain(blockchain common.Blockchain) ConfigOption {
	return func(opts *configOptions) {
		opts.Blockchain = blockchain
	}
}

func WithNetwork(network common.Network) ConfigOption {
	return func(opts *configOptions) {
		opts.Network = network
	}
}

func WithEnvironment(env Env) ConfigOption {
	return func(opts *configOptions) {
		opts.Env = env
	}
}

func mapToChainStorageEnv(env Env) sdk.Env {
	switch env {
	case EnvLocal, EnvDevelopment:
		return sdk.EnvDevelopment
	case EnvProduction:
		return sdk.EnvProduction
	default:
		return ""
	}
}

func (c *Config) Env() Env {
	return c.env
}

func (c *Config) Blockchain() common.Blockchain {
	return c.Chain.Blockchain
}

func (c *Config) Network() common.Network {
	return c.Chain.Network
}

func (c *Config) Tier() int {
	return c.SLA.Tier
}

func (c *Config) IsTest() bool {
	return os.Getenv(EnvVarTestType) != ""
}

func (c *Config) IsIntegrationTest() bool {
	return os.Getenv(EnvVarTestType) == "integration"
}

func (c *Config) IsFunctionalTest() bool {
	return os.Getenv(EnvVarTestType) == "functional"
}

func (c *Config) IsCI() bool {
	return os.Getenv(EnvVarCI) != ""
}

func (c *Config) GetCommonTags() map[string]string {
	return map[string]string{
		tagBlockchain: c.Blockchain().GetName(),
		tagNetwork:    c.Network().GetName(),
		tagTier:       strconv.Itoa(c.Tier()),
	}
}

// setDerivedConfigs recursively calls DeriveConfig on all the derivedConfig.
func (c *Config) setDerivedConfigs(v reflect.Value) {
	if v.CanInterface() {
		if oc, ok := v.Interface().(derivedConfig); ok {
			oc.DeriveConfig(c)
			return
		}
	}

	elem := v.Elem()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.Kind() == reflect.Struct {
			c.setDerivedConfigs(field.Addr())
		}
	}
}

func (c *TableConfig) GetSupportedFormats() map[string]bool {
	supportedFormats := make(map[string]bool)

	for _, format := range c.SupportedFormats {
		supportedFormats[format] = true
	}

	return supportedFormats
}

func (c *StreamTableConfig) GetParallelism() int {
	if c.Parallelism < 1 {
		return defaultStreamParallelism
	}

	return c.Parallelism
}

func (c *ChainStorageSDKConfig) DeriveConfig(cfg *Config) {
	c.Config.Blockchain = cfg.Blockchain()
	c.Config.Network = cfg.Network()
	c.Config.Env = mapToChainStorageEnv(cfg.Env())
}

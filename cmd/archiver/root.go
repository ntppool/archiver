package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kong"
	"go.ntppool.org/archiver/config"
)

var globalConfig *config.Config

// CLI represents the command line interface
type CLI struct {
	Archive ArchiveCmd `cmd:"archive" help:"Archive log scores"`
}

// ArchiveCmd represents the archive command
type ArchiveCmd struct {
	Table string `short:"t" default:"log_scores" help:"Table to pull data from"`
}

// Run executes the archive command
func (cmd *ArchiveCmd) Run() error {
	return runArchive(cmd.Table, globalConfig)
}

// Execute parses command line arguments and executes the appropriate command
func Execute() {
	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	globalConfig = cfg

	// Parse CLI
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("archiver"),
		kong.Description("Archive NTP Pool log scores"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": cfg.App.Version,
		},
	)

	// Execute the command
	err = ctx.Run()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

// loadConfig loads and validates the configuration
func loadConfig() (*config.Config, error) {
	var cfg config.Config

	// Parse with Kong to get environment variables
	parser, err := kong.New(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating parser: %w", err)
	}

	// Parse empty args to load from environment
	_, err = parser.Parse([]string{})
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Post-process and validate
	if err := cfg.PostProcess(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// getGlobalConfig returns the global configuration
func getGlobalConfig() *config.Config {
	return globalConfig
}

package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/source"
	"go.ntppool.org/archiver/storage"
)

// archiveCmd represents the archive command
var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "archive",
	Long:  `This subcommand archives log scores`,
	// Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		table := cmd.Flag("table").Value.String()
		if len(table) == 0 {
			table = "log_scores"
		}

		// month, err := strconv.Atoi(args[0])
		err := runArchive(table)
		if err != nil {
			log.Fatalf("archive error: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(archiveCmd)
	archiveCmd.Flags().StringP("table", "t", "log_scores", "Table to pull data from")
}

func runArchive(table string) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", os.Getenv("db_user"), os.Getenv("db_pass"),
		os.Getenv("db_host"), os.Getenv("db_database"),
	)

	err := db.Setup(dsn)
	if err != nil {
		return fmt.Errorf("database connection: %s", err)
	}

	if err = db.DB.Ping(); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	// todo: make this be a goroutine that waits for a signal to release the lock
	lock := getLock("archiver-" + os.Getenv("db_database"))
	if !lock {
		return fmt.Errorf("did not get lock, exiting")
	}

	status, err := storage.GetArchiveStatus()
	if err != nil {
		return fmt.Errorf("archive status: %s", err)
	}

	// todo: manage the config better instead of having os.Getenv() everywhere
	retentionDays := 15
	retentionDaysStr := os.Getenv("retention_days")
	if len(retentionDaysStr) > 0 {
		if i, err := strconv.Atoi(retentionDaysStr); err == nil && i > 0 {
			retentionDays = i
		}
	}

	source := source.New(table, retentionDays)
	for _, s := range status {

		if s.Archiver == "cleanup" {
			err = source.Cleanup(s)
			if err != nil {
				log.Printf("error running cleanup: %s", err)
			}
			continue
		}

		err := source.Process(s)
		if err != nil {
			return fmt.Errorf("error processing %s: %s", s.Archiver, err)
		}

	}

	return nil
}

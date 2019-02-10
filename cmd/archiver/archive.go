package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ntppool/archiver/db"
	"github.com/ntppool/archiver/source"
	"github.com/ntppool/archiver/storage"
	"github.com/spf13/cobra"
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

	// todo: make this be a goroutine that waits for a signal to release the lock
	lock := getLock()
	if !lock {
		return fmt.Errorf("Did not get lock, exiting")
	}

	status, err := storage.GetArchiveStatus()
	if err != nil {
		return fmt.Errorf("archive status: %s", err)
	}

	source := source.New(table)

	for _, s := range status {
		err := source.Process(s)
		if err != nil {
			return fmt.Errorf("error processing %s: %s", s.Archiver, err)
		}
	}

	return nil
}

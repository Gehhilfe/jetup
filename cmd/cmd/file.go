/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"

	"github.com/gehhilfe/jetup"
	"github.com/gehhilfe/jetup/stores"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// fileCmd represents the file command
var fileCmd = &cobra.Command{
	Use:   "file",
	Short: "Backup stream to file system",
	Long:  `Backup a stream to file system backup store.`,
	Run: func(cmd *cobra.Command, args []string) {
		stream, _ := cmd.Flags().GetString("stream")
		outputDir, _ := cmd.Flags().GetString("output")

		store := stores.NewFileBackupStore(outputDir)
		j := jetup.New(stores.NewLoggingStore(store))
		nc, err := nats.Connect("nats://localhost:4222")
		if err != nil {
			panic(err)
		}
		err = j.BackupStream(context.Background(), nc, stream)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	backupCmd.AddCommand(fileCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// fileCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	fileCmd.Flags().StringP("output", "o", "", "Path to backup directory (required)")
	fileCmd.MarkFlagRequired("output")
}

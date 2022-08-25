/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gehhilfe/jetup"
	"github.com/gehhilfe/jetup/stores"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// blobCmd represents the blob command
var blobCmd = &cobra.Command{
	Use:   "blob",
	Short: "Backup stream to azure blob storage",
	Run: func(cmd *cobra.Command, args []string) {
		accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
		if !ok {
			panic("Set AZURE_STORAGE_ACCOUNT_NAME")
		}

		accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
		if !ok {
			panic("Set AZURE_STORAGE_ACCOUNT_KEY")
		}

		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			panic(err)
		}

		serviceClient, err := azblob.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), cred, nil)
		if err != nil {
			panic(err)
		}

		container, _ := cmd.Flags().GetString("container")
		cc, err := serviceClient.NewContainerClient(container)
		if err != nil {
			panic(err)
		}

		prefix, _ := cmd.Flags().GetString("prefix")

		store := stores.NewBlobStore(cc, prefix)

		server, _ := cmd.Flags().GetString("server")
		j := jetup.New(stores.NewLoggingStore(store))
		nc, err := nats.Connect(server)
		if err != nil {
			panic(err)
		}

		stream, _ := cmd.Flags().GetString("stream")

		err = j.BackupStream(context.Background(), nc, stream)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	backupCmd.AddCommand(blobCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// blobCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	blobCmd.Flags().StringP("container", "c", "", "container name (required)")
	blobCmd.MarkFlagRequired("container")

	blobCmd.Flags().StringP("prefix", "p", "backup/", "blob name prefix")
}

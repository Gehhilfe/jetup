/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gehhilfe/jetup"
	"github.com/gehhilfe/jetup/stores"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// s3Cmd represents the s3 command
var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Backup stream to aws s3",
	Long:  `Backup a stream to an aws s3 bucket.`,
	Run: func(cmd *cobra.Command, args []string) {
		stream, _ := cmd.Flags().GetString("stream")
		bucket, _ := cmd.Flags().GetString("bucket")
		prefix, _ := cmd.Flags().GetString("prefix")

		region := "eu-central-1" // is ignored s3 is global
		mySession := session.Must(session.NewSession(&aws.Config{
			Region: &region,
		}))

		store := stores.NewS3BackupStore(s3.New(mySession), bucket, prefix)
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
	backupCmd.AddCommand(s3Cmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// s3Cmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	fileCmd.Flags().StringP("bucket", "b", "", "S3 bucket name (required)")
	fileCmd.MarkFlagRequired("bucket")

	fileCmd.Flags().StringP("prefix", "p", "backup/", "S3 object key prefix")
}

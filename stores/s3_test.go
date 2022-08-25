package stores

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3(t *testing.T) {
	region := "eu-central-1"
	mySession := session.Must(session.NewSession(&aws.Config{
		Region: &region,
	}))

	store := NewS3BackupStore(s3.New(mySession), "test-backup-213", "nice/backup/prefix").(*S3Store)

	for i := 0; i < 5; i++ {
		backup, err := store.NewBackup(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 10; i++ {
			pkg, err := backup.NewPackage(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < 100; i++ {
				pkg.AddMessage(randomMessage())
			}
			err = backup.CommitPackage(context.Background(), pkg)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

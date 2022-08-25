package stores

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func TestBlob(t *testing.T) {
	accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
	if !ok {
		t.Fatal("Set AZURE_STORAGE_ACCOUNT_NAME")
	}

	accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
	if !ok {
		t.Fatal("Set AZURE_STORAGE_ACCOUNT_KEY")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Fatal(err)
	}

	serviceClient, err := azblob.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), cred, nil)
	if err != nil {
		t.Fatal(err)
	}

	cc, err := serviceClient.NewContainerClient("testc")
	if err != nil {
		panic(err)
	}

	store := NewBlobStore(cc, "test-backup")

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

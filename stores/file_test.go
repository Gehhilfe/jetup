package stores

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/gehhilfe/jetup"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randomMessage() *jetup.Message {
	headerKey := randStringRunes(10)
	return &jetup.Message{
		Subject: randStringRunes(64),
		Body:    []byte(randStringRunes(1024)),
		Headers: map[string][]string{
			headerKey: {randStringRunes(8), randStringRunes(8)},
		},
	}
}

func expectDirCount(t *testing.T, path string, count int) {
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != count {
		t.Errorf("Expected %d directory entries, found %d", count, len(entries))
	}
}

func TestFileStoreShouldCreateNewFilesForEachPackage(t *testing.T) {
	dir := t.TempDir()

	store := NewFileBackupStore(dir).(*FileBackupStore)

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

	expectDirCount(t, store.path, 5)
	expectDirCount(t, filepath.Join(store.path, "0"), 10)
	expectDirCount(t, filepath.Join(store.path, "1"), 10)
	expectDirCount(t, filepath.Join(store.path, "2"), 10)
	expectDirCount(t, filepath.Join(store.path, "3"), 10)
	expectDirCount(t, filepath.Join(store.path, "4"), 10)
}

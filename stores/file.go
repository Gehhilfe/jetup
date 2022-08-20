package stores

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gehhilfe/jetup"
	"github.com/vmihailenco/msgpack/v5"
)

type FileBackupStore struct {
	path string
}

type FileBackupStoreEntry struct {
	path string
}

type FileBackupPackage struct {
	path     string
	messages []*jetup.Message
}

func (s *FileBackupStore) NewBackup(ctx context.Context) (jetup.BackupStoreEntry, error) {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		return nil, fmt.Errorf("cant read directory: %w", err)
	}

	// find highest not used number for next backup
	highest := int64(-1)
	for _, v := range entries {
		if v.IsDir() {
			nameAsInt, err := strconv.ParseInt(v.Name(), 10, 64)
			if err != nil {
				// not parsable as int, therefore ignore folder
				continue
			}

			if highest < nameAsInt {
				highest = nameAsInt
			}
		}
	}

	highest += 1

	folderName := strconv.FormatInt(highest, 10)

	return &FileBackupStoreEntry{path: filepath.Join(s.path, folderName)}, nil
}

func (e *FileBackupStoreEntry) NewPackage(ctx context.Context) (jetup.BackupPackage, error) {
	if _, err := os.Stat(e.path); err != nil {
		// First package
		return &FileBackupPackage{path: filepath.Join(e.path, "0.bak")}, nil
	}

	entries, err := os.ReadDir(e.path)
	if err != nil {
		return nil, fmt.Errorf("cant read directory: %w", err)
	}

	// find highest not used number for next backup
	highest := int64(-1)
	for _, v := range entries {
		if !v.IsDir() {
			split := strings.Split(v.Name(), ".")
			nameAsInt, err := strconv.ParseInt(split[0], 10, 64)
			if err != nil {
				// not parsable as int, therefore ignore folder
				continue
			}

			if highest < nameAsInt {
				highest = nameAsInt
			}
		}
	}

	highest += 1
	fileName := strconv.FormatInt(highest, 10) + ".bak"

	return &FileBackupPackage{path: filepath.Join(e.path, fileName)}, nil
}

func (e *FileBackupStoreEntry) CommitPackage(ctx context.Context, pkg jetup.BackupPackage) error {
	fpkgm, ok := pkg.(*FileBackupPackage)
	if !ok {
		return errors.New("only FileBackupPackage are supported")
	}

	if _, err := os.Stat(e.path); err != nil {
		err := os.MkdirAll(e.path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create store entry directory: %w", err)
		}
	}

	file, err := os.Create(fpkgm.path)
	if err != nil {
		return fmt.Errorf("could not create package file: %w", err)
	}
	defer file.Close()

	marshalled, err := msgpack.Marshal(fpkgm.messages)
	if err != nil {
		return fmt.Errorf("could not msgpack messages: %w", err)
	}

	_, err = file.Write(marshalled)
	if err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	return nil
}

func (p *FileBackupPackage) AddMessage(msg *jetup.Message) error {
	p.messages = append(p.messages, msg)
	return nil
}

var _ jetup.BackupPackage = &FileBackupPackage{}
var _ jetup.BackupStoreEntry = &FileBackupStoreEntry{}
var _ jetup.BackupStore = &FileBackupStore{}

func NewFileBackupStore(path string) jetup.BackupStore {
	return &FileBackupStore{
		path: path,
	}
}

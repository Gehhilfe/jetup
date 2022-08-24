package stores

import (
	"context"
	"errors"
	"strconv"

	"github.com/gehhilfe/jetup"
	log "github.com/sirupsen/logrus"
)

type LoggingStore struct {
	backend jetup.BackupStore
}

type LoggingEntry struct {
	entry jetup.BackupStoreEntry
}

type LoggingPackage struct {
	pkg jetup.BackupPackage
	ctr int
}

// AddMessage implements jetup.BackupPackage
func (p *LoggingPackage) AddMessage(msg *jetup.Message) error {
	err := p.pkg.AddMessage(msg)
	if err != nil {
		return err
	}
	p.ctr += 1
	return nil
}

// CommitPackage implements jetup.BackupStoreEntry
func (e *LoggingEntry) CommitPackage(ctx context.Context, pkg jetup.BackupPackage) error {
	lpkg, ok := pkg.(*LoggingPackage)
	if !ok {
		return errors.New("expected logging package")
	}

	err := e.entry.CommitPackage(ctx, lpkg.pkg)
	if err != nil {
		log.WithError(err).Error("Error committing backup package")
		return err
	}

	log.WithField("messages", strconv.Itoa(lpkg.ctr)).Info("Committed package to backup store")

	return nil
}

// NewPackage implements jetup.BackupStoreEntry
func (e *LoggingEntry) NewPackage(ctx context.Context) (jetup.BackupPackage, error) {
	pkg, err := e.entry.NewPackage(ctx)
	if err != nil {
		log.WithError(err).Error("Error creating new backup package")
		return nil, err
	}

	log.Info("Created a new backup package")

	return &LoggingPackage{
		pkg: pkg,
		ctr: 0,
	}, nil
}

// NewBackup implements jetup.BackupStore
func (s *LoggingStore) NewBackup(ctx context.Context) (jetup.BackupStoreEntry, error) {
	entry, err := s.backend.NewBackup(ctx)
	if err != nil {
		log.WithError(err).Error("Error creating new backup")
		return nil, err
	}

	log.Info("Created a new backup")

	return &LoggingEntry{
		entry: entry,
	}, nil
}

func NewLoggingStore(store jetup.BackupStore) jetup.BackupStore {
	return &LoggingStore{
		backend: store,
	}
}

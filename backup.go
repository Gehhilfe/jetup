package jetup

import "context"

type Message struct {
	Subject string
	Body    []byte
	Headers map[string][]string
}

// BackupStore allows storage of multiple backups in backup target, e.g. filesystem, s3 or other.
type BackupStore interface {
	// Creates a new backup. Careful creation can be delayed until first packages is committed.
	// A new backup should only create after commit a package to previously created backup.
	NewBackup(ctx context.Context) (BackupStoreEntry, error)
}

type BackupStoreEntry interface {
	// Creates a new package, that can be committed afterwards to the backup.
	NewPackage(ctx context.Context) (BackupPackage, error)

	// Commit a package. Actually writes data to the backup target.
	CommitPackage(ctx context.Context, pkg BackupPackage) error
}

// BackupPackage combines multiple messages into single package.
type BackupPackage interface {
	AddMessage(msg *Message) error
}

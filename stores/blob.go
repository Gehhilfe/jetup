package stores

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/gehhilfe/jetup"
	"github.com/vmihailenco/msgpack/v5"
)

type BlobStore struct {
	client *azblob.ContainerClient
	prefix string
}

type BlobEntry struct {
	store *BlobStore
	num   int64
}

type BlobStorePackage struct {
	num      int64
	messages []*jetup.Message
}

// AddMessage implements jetup.BackupPackage
func (p *BlobStorePackage) AddMessage(msg *jetup.Message) error {
	p.messages = append(p.messages, msg)
	return nil
}

// CommitPackage implements jetup.BackupStoreEntry
func (e *BlobEntry) CommitPackage(ctx context.Context, pkg jetup.BackupPackage) error {
	spkg, ok := pkg.(*BlobStorePackage)
	if !ok {
		return errors.New("only BlobStorePackage are supported")
	}

	data, err := msgpack.Marshal(spkg.messages)
	if err != nil {
		return fmt.Errorf("could not msgpack messages: %w", err)
	}

	key := strings.Join([]string{
		e.store.prefix,
		strconv.FormatInt(e.num, 10),
		strconv.FormatInt(spkg.num, 10) + ".bak",
	}, "/")
	client, err := e.store.client.NewBlockBlobClient(key)
	if err != nil {
		return err
	}
	_, err = client.UploadBuffer(ctx, data, azblob.UploadOption{TagsMap: map[string]string{
		"uploader": "jetup",
	}})
	return err
}

// NewPackage implements jetup.BackupStoreEntry
func (e *BlobEntry) NewPackage(ctx context.Context) (jetup.BackupPackage, error) {
	highest := int64(-1)
	psplit := strings.Split(e.store.prefix, "/")
	findex := len(psplit) + 1
	prefix := strings.Join([]string{e.store.prefix, strconv.FormatInt(e.num, 10)}, "/")
	pager := e.store.client.ListBlobsFlat(&azblob.ContainerListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, v := range resp.ListBlobsFlatSegmentResponse.Segment.BlobItems {
			if !strings.HasSuffix(*v.Name, ".bak") {
				continue
			}
			ksplit := strings.Split((*v.Name), "/")
			if findex >= len(ksplit) {
				return nil, errors.New("wrong key format")
			}
			f := ksplit[findex]
			num, err := strconv.ParseInt(f, 10, 64)
			if err != nil {
				continue
			}
			if num > highest {
				highest = num
			}
		}
	}
	highest += 1
	return &BlobStorePackage{
		num: highest,
	}, nil
}

// NewBackup implements jetup.BackupStore
func (s *BlobStore) NewBackup(ctx context.Context) (jetup.BackupStoreEntry, error) {
	highest := int64(-1)
	psplit := strings.Split(s.prefix, "/")
	findex := len(psplit)
	pager := s.client.ListBlobsFlat(&azblob.ContainerListBlobsFlatOptions{
		Prefix: &s.prefix,
	})
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, v := range resp.ListBlobsFlatSegmentResponse.Segment.BlobItems {
			ksplit := strings.Split((*v.Name), "/")
			if findex >= len(ksplit) {
				return nil, errors.New("wrong key format")
			}
			f := ksplit[findex]
			num, err := strconv.ParseInt(f, 10, 64)
			if err != nil {
				continue
			}
			if num > highest {
				highest = num
			}
		}
	}
	highest += 1
	return &BlobEntry{
		store: s,
		num:   highest,
	}, nil
}

func NewBlobStore(client *azblob.ContainerClient, prefix string) jetup.BackupStore {
	return &BlobStore{
		client: client,
		prefix: prefix,
	}
}

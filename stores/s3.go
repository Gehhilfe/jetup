package stores

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gehhilfe/jetup"
	"github.com/vmihailenco/msgpack/v5"
)

type S3Store struct {
	awsS3  *s3.S3
	bucket string
	prefix string
}

type S3StoreEntry struct {
	store *S3Store
	num   int64
}

type S3StorePackage struct {
	num      int64
	messages []*jetup.Message
}

// AddMessage implements jetup.BackupPackage
func (p *S3StorePackage) AddMessage(msg *jetup.Message) error {
	p.messages = append(p.messages, msg)
	return nil
}

// CommitPackage implements jetup.BackupStoreEntry
func (e *S3StoreEntry) CommitPackage(ctx context.Context, pkg jetup.BackupPackage) error {
	spkg, ok := pkg.(*S3StorePackage)
	if !ok {
		return errors.New("only FileBackupPackage are supported")
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
	_, err = e.store.awsS3.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: &e.store.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

// NewPackage implements jetup.BackupStoreEntry
func (e *S3StoreEntry) NewPackage(ctx context.Context) (jetup.BackupPackage, error) {
	highest := int64(-1)
	psplit := strings.Split(e.store.prefix, "/")
	findex := len(psplit) + 1
	err := e.store.iterateAllObjects(ctx, strings.Join([]string{e.store.prefix, strconv.FormatInt(e.num, 10)}, "/"), func(obj *s3.Object) error {
		if obj.Key == nil {
			return nil
		}
		if !strings.HasSuffix(*obj.Key, ".bak") {
			return nil
		}
		ksplit := strings.Split((*obj.Key), "/")
		if findex >= len(ksplit) {
			return errors.New("wrong key format")
		}
		f := ksplit[findex]
		fsplit := strings.Split(f, ".")
		num, err := strconv.ParseInt(fsplit[0], 10, 64)
		if err != nil {
			return nil
		}

		if num > highest {
			highest = num
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	highest += 1
	return &S3StorePackage{
		num: highest,
	}, nil
}

func (s *S3Store) iterateAllObjects(ctx context.Context, prefix string, fn func(obj *s3.Object) error) error {
	var ct *string = nil
	for {
		res, err := s.awsS3.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            &s.bucket,
			Prefix:            &prefix,
			ContinuationToken: ct,
		})
		if err != nil {
			return err
		}
		ct = res.NextContinuationToken
		for _, v := range res.Contents {
			err = fn(v)
			if err != nil {
				return err
			}
		}
		if res.IsTruncated == nil || !*res.IsTruncated {
			return nil
		}
	}
}

func (s *S3Store) NewBackup(ctx context.Context) (jetup.BackupStoreEntry, error) {
	highest := int64(-1)
	psplit := strings.Split(s.prefix, "/")
	findex := len(psplit)
	err := s.iterateAllObjects(ctx, s.prefix, func(obj *s3.Object) error {
		if obj.Key == nil {
			return nil
		}
		ksplit := strings.Split((*obj.Key), "/")
		if findex >= len(ksplit) {
			return errors.New("wrong key format")
		}
		f := ksplit[findex]
		num, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return nil
		}

		if num > highest {
			highest = num
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	highest += 1
	return &S3StoreEntry{
		store: s,
		num:   highest,
	}, nil
}

func NewS3BackupStore(s *s3.S3, bucket, prefix string) jetup.BackupStore {
	return &S3Store{
		awsS3:  s,
		bucket: bucket,
		prefix: prefix,
	}
}

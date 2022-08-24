package jetup

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type jetup struct {
	store BackupStore
}

func New(store BackupStore) *jetup {
	return &jetup{
		store: store,
	}
}

func (j *jetup) BackupStream(ctx context.Context, nc *nats.Conn, stream string) error {
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("open jetstream context: %w", err)
	}

	_, err = js.ConsumerInfo(stream, "jetup")
	if err != nil {
		if err == nats.ErrConsumerNotFound {
			_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
				Durable:       "jetup",
				Description:   "Jetup backup consumer",
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckAllPolicy,
				AckWait:       10 * time.Minute,
				MaxAckPending: 100,
			})
			if err != nil {
				return fmt.Errorf("creating consumer: %w", err)
			}
		} else {
			return fmt.Errorf("get consumer info: %w", err)
		}
	}

	sub, err := js.PullSubscribe("", "jetup", nats.BindStream(stream))
	if err != nil {
		return fmt.Errorf("pullsubscribe: %w", err)
	}

	entry, err := j.store.NewBackup(ctx)
	if err != nil {
		return fmt.Errorf("new backup: %w", err)
	}

	for {
		msg, err := sub.Fetch(100, nats.MaxWait(time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				break
			} else {
				return fmt.Errorf("fetch: %w", err)
			}
		}

		pkg, err := entry.NewPackage(ctx)
		if err != nil {
			return fmt.Errorf("new package: %w", err)
		}

		for _, v := range msg {
			err = pkg.AddMessage(&Message{
				Subject: v.Subject,
				Body:    v.Data,
				Headers: v.Header,
			})
			if err != nil {
				return fmt.Errorf("add message: %w", err)
			}
		}

		err = entry.CommitPackage(ctx, pkg)
		if err != nil {
			return fmt.Errorf("commit package: %w", err)
		}

		lastMessage := msg[len(msg)-1]
		err = lastMessage.AckSync()
		if err != nil {
			return fmt.Errorf("ack message: %w", err)
		}

		if len(msg) > 100 {
			break
		}
	}
	return nil
}

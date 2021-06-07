package data

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Mario-Jimenez/newspub/publisher"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type Data interface {
	Find(context.Context) (map[string][]string, error)
}

type Handler struct {
	wg *sync.WaitGroup

	data       Data
	publishers map[string]publisher.Publisher
}

// NewHandler creates a handler for data management
func NewHandler(data Data, publishers map[string]publisher.Publisher) *Handler {
	return &Handler{
		wg:         &sync.WaitGroup{},
		data:       data,
		publishers: publishers,
	}
}

func (h *Handler) PublishMessages(ctx context.Context) error {
	news, err := h.data.Find(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for portal, links := range news {
		publisher, ok := h.publishers[portal]
		if ok {
			h.wg.Add(1)
			go h.portalPublisher(ctx, publisher, links)
			continue
		}
		log.WithFields(log.Fields{
			"portal": portal,
		}).Error("portal publisher not found")
	}

	h.wg.Wait()

	return nil
}

func (h *Handler) portalPublisher(ctx context.Context, publisher publisher.Publisher, links []string) {
	defer func() {
		h.wg.Done()
	}()

	for _, link := range links {
		msg := map[string]interface{}{
			"news_url": link,
		}
		b, err := json.Marshal(msg)
		if err != nil {
			log.WithFields(log.Fields{
				"game":  link,
				"error": err.Error(),
			}).Error("json marshal failed")

			continue
		}

		h.publishMessage(ctx, publisher, b)
	}
}

func (h *Handler) publishMessage(ctx context.Context, publisher publisher.Publisher, message []byte) {
	// publish a message
	wait := 1
	for {
		if err := publisher.Publish(ctx, message); err != nil {
			log.WithFields(log.Fields{
				"message": string(message),
				"wait":    fmt.Sprintf("Retrying in %d second(s)", wait),
				"error":   err.Error(),
			}).Warning("Failed to publish message. Retrying...")
			time.Sleep(time.Duration(wait) * time.Second)
			if wait <= 60 {
				wait += 3
			}
			continue
		}

		log.WithFields(log.Fields{
			"message": string(message),
		}).Debug("Message published successfully")

		return
	}
}

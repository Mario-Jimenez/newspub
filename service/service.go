package service

import (
	"context"

	"github.com/Mario-Jimenez/newspub/broker/kafka"
	"github.com/Mario-Jimenez/newspub/config"
	"github.com/Mario-Jimenez/newspub/data"
	"github.com/Mario-Jimenez/newspub/logger"
	"github.com/Mario-Jimenez/newspub/news"
	"github.com/Mario-Jimenez/newspub/publisher"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// Run service
func Run(serviceName, serviceVersion string) {
	// load app configuration
	conf, err := config.NewFileConfig()
	if err != nil {
		if errors.IsNotFound(err) {
			log.WithFields(log.Fields{
				"error": errors.Details(err),
			}).Error("Configuration file not found")
			return
		}
		if errors.IsNotValid(err) {
			log.WithFields(log.Fields{
				"error": errors.Details(err),
			}).Error("Invalid configuration values")
			return
		}
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to retrieve secrets")
		return
	}

	// initialize logger
	logger.InitializeLogger(serviceName, serviceVersion, conf.Values().LogLevel)

	producerTeletica := kafka.NewProducer("newsteletica", conf.Values().KafkaConnection)
	producerRepretel := kafka.NewProducer("newsrepretel", conf.Values().KafkaConnection)
	producerCRHoy := kafka.NewProducer("newscrhoy", conf.Values().KafkaConnection)
	producerDiarioExtra := kafka.NewProducer("newsdiarioextra", conf.Values().KafkaConnection)

	producers := map[string]publisher.Publisher{
		"teletica":    producerTeletica,
		"repretel":    producerRepretel,
		"crhoy":       producerCRHoy,
		"diarioextra": producerDiarioExtra,
	}

	newsData := news.NewFileHandler()

	dataHandler := data.NewHandler(newsData, producers)
	err = dataHandler.PublishMessages(context.Background())
	if err != nil {
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to publish messages")
	}

	if err := producerTeletica.Close(); err != nil {
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to close publisher")
	}

	if err := producerRepretel.Close(); err != nil {
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to close publisher")
	}

	if err := producerCRHoy.Close(); err != nil {
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to close publisher")
	}

	if err := producerDiarioExtra.Close(); err != nil {
		log.WithFields(log.Fields{
			"error": errors.Details(err),
		}).Error("Failed to close publisher")
	}
}

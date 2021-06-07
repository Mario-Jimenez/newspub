package news

import (
	"context"
	"encoding/json"
	"os"

	"github.com/juju/errors"
)

type FileHandler struct{}

func NewFileHandler() *FileHandler {
	return &FileHandler{}
}

func (*FileHandler) Find(ctx context.Context) (map[string][]string, error) {
	file, err := os.ReadFile("./news/news.json")
	if err != nil {
		return nil, errors.New(err.Error())
	}

	var data map[string][]string
	err = json.Unmarshal(file, &data)
	if err != nil {
		return nil, errors.NotValidf(err.Error())
	}

	return data, nil
}

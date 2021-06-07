build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o newspub-srv main.go

run:
	./newspub-srv

docker-build:
	docker build . -t docker.pkg.github.com/mario-jimenez/newspub/newspub-srv:0.0.1

docker-push:
	docker push docker.pkg.github.com/mario-jimenez/newspub/newspub-srv:0.0.1

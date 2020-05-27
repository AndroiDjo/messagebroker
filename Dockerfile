FROM golang:1.14-alpine AS build
RUN apk add --no-cache git gcc musl-dev
WORKDIR /src
RUN mkdir /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY ./ ./
RUN cd /src && go build -o ./bin/mbs ./cmd

FROM alpine
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
COPY --from=build /src/bin/mbs /usr/bin/mbs
EXPOSE 80
CMD ["mbs", "-listen-addr=:80"]
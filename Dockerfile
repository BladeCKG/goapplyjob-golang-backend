FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/migrate ./cmd/migrate
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/watcher ./cmd/watcher
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/importer ./cmd/importer
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/rawjobworker ./cmd/rawjobworker
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/parsedjobworker ./cmd/parsedjobworker
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/parsedfreshness ./cmd/parsedfreshness
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/skippablerecheck ./cmd/skippablerecheck

FROM alpine:3.21

WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=build /out/api /app/api
COPY --from=build /out/migrate /app/migrate
COPY --from=build /out/watcher /app/watcher
COPY --from=build /out/importer /app/importer
COPY --from=build /out/rawjobworker /app/rawjobworker
COPY --from=build /out/parsedjobworker /app/parsedjobworker
COPY --from=build /out/parsedfreshness /app/parsedfreshness
COPY --from=build /out/skippablerecheck /app/skippablerecheck

EXPOSE 8080

CMD ["/app/api"]

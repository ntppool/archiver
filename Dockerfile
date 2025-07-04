FROM golang:1.24-alpine3.22 AS build
RUN apk add git

WORKDIR /app
ADD . /app

RUN go install ./cmd/archiver

FROM alpine:3.22
RUN apk --no-cache add ca-certificates tzdata zsh jq tmux

RUN addgroup np && adduser -D -G np np
RUN touch ~np/.zshrc ~root/.zshrc; chown np:np ~np/.zshrc

WORKDIR /archiver
COPY --from=build /go/bin/archiver /archiver/
ADD archive-continuously /archiver/

USER np

EXPOSE 5000

CMD ["/archiver/archiver"]

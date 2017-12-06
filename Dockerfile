FROM golang:1.9-alpine3.6 AS build
WORKDIR /go/src/github.com/ntppool/archiver/

ADD . /go/src/github.com/ntppool/archiver/
RUN go-wrapper install

FROM alpine:3.6
RUN apk --no-cache add ca-certificates

RUN addgroup np && adduser -D -G np np

#WORKDIR /ntppool/archiver
COPY --from=build /go/bin/archiver /archiver

USER np

CMD ["/archiver"]

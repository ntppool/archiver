FROM golang:1.11.5-alpine3.9 AS build

WORKDIR /go/src/github.com/ntppool/archiver
ADD . /go/src/github.com/ntppool/archiver
RUN go install

#FROM node:8 AS clientbuild
#WORKDIR /bearbank/
#COPY ./client/package*.json ./
#RUN npm install
#ADD ./client/ ./
#RUN rm public/*~
#RUN npm run build

FROM alpine:3.9
RUN apk --no-cache add ca-certificates tzdata

RUN addgroup np && adduser -D -G np np

WORKDIR /archiver
#RUN mkdir /etc/spamsources/
COPY --from=build /go/bin/archiver /archiver/

#COPY --from=clientbuild /np/build/ /np/client/build/
#COPY --from=build /go/src/git.develooper.com/spamsources/templates /spamsources/templates/
#COPY --from=build /go/src/git.develooper.com/spamsources/static /spamsources/static/
#COPY --from=build /go/src/git.develooper.com/spamsources/config.yaml.sample /etc/spamsources/config.yaml

USER np

EXPOSE 5000

CMD ["/archiver/archiver"]

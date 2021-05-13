FROM registry.trendyol.com/platform/base/image/golang:1.16 AS builder

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=amd64
ENV GO111MODULE=on

ADD . ./app

WORKDIR app

RUN apt-get update
RUN apt-get -y install libpcap-dev libpcap0.8 libpcap0.8-dev
RUN go mod vendor
RUN go build -a -o .

FROM registry.trendyol.com/platform/base/image/golang:1.16
RUN apt-get update
RUN apt-get -y install libpcap-dev libpcap0.8 libpcap0.8-dev
ENV LANG C.UTF-8
ENV GOPATH /go

COPY --from=builder /go/app/pg-dml-replay /app/pg-dml-replay
#COPY --from=builder /go/app/conf/ /app/conf/
WORKDIR /app

RUN chmod +x pg-dml-replay
ENTRYPOINT ["./pg-dml-replay"]

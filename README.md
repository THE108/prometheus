# About this fork

This fork is intended to implement custom etcd service discovery.

Targets must be provided as a key/value pairs in etcd. Key example:

Key:

    /some_key_prefix/some_datacenter/some_api
    
Value:

    {"url": "http://some_api:5000/metrics", "tags": [{"key": "some_key", "value": "some_value"}]}

See `./retrieval/discovery/etcd.go` file.

Etcd service discovery configuration example:

    global:
      scrape_interval:     15s
      evaluation_interval: 30s
    
    scrape_configs:
    - job_name: test-service
    
      etcd_sd_configs:
      - endpoints:
        - 'http://192.168.99.100:4001/'
        key_prefix: '/some_key_prefix'

# Prometheus [![Build Status](https://travis-ci.org/prometheus/prometheus.svg)][travis]

[![CircleCI](https://circleci.com/gh/prometheus/prometheus/tree/master.svg?style=shield)][circleci]
[![Docker Repository on Quay](https://quay.io/repository/prometheus/prometheus/status)][quay]
[![Docker Pulls](https://img.shields.io/docker/pulls/prom/prometheus.svg?maxAge=604800)][hub]
[![Go Report Card](https://goreportcard.com/badge/github.com/prometheus/prometheus)](https://goreportcard.com/report/github.com/prometheus/prometheus)
[![Code Climate](https://codeclimate.com/github/prometheus/prometheus/badges/gpa.svg)](https://codeclimate.com/github/prometheus/prometheus)
[![Issue Count](https://codeclimate.com/github/prometheus/prometheus/badges/issue_count.svg)](https://codeclimate.com/github/prometheus/prometheus)

Visit [prometheus.io](https://prometheus.io) for the full documentation,
examples and guides.

Prometheus, a [Cloud Native Computing Foundation](https://cncf.io/) project, is a systems and service monitoring system. It collects metrics
from configured targets at given intervals, evaluates rule expressions,
displays the results, and can trigger alerts if some condition is observed
to be true.

Prometheus' main distinguishing features as compared to other monitoring systems are:

- a **multi-dimensional** data model (timeseries defined by metric name and set of key/value dimensions)
- a **flexible query language** to leverage this dimensionality
- no dependency on distributed storage; **single server nodes are autonomous**
- timeseries collection happens via a **pull model** over HTTP
- **pushing timeseries** is supported via an intermediary gateway
- targets are discovered via **service discovery** or **static configuration**
- multiple modes of **graphing and dashboarding support**
- support for hierarchical and horizontal **federation**

## Architecture overview

![](https://cdn.rawgit.com/prometheus/prometheus/e761f0d/documentation/images/architecture.svg)

## Install

There are various ways of installing Prometheus.

### Precompiled binaries

Precompiled binaries for released versions are available in the
[*download* section](https://prometheus.io/download/)
on [prometheus.io](https://prometheus.io). Using the latest production release binary
is the recommended way of installing Prometheus.
See the [Installing](https://prometheus.io/docs/introduction/install/)
chapter in the documentation for all the details.

Debian packages [are available](https://packages.debian.org/sid/net/prometheus).

### Docker images

Docker images are available on [Quay.io](https://quay.io/repository/prometheus/prometheus).

### Building from source

To build Prometheus from the source code yourself you need to have a working
Go environment with [version 1.5 or greater installed](http://golang.org/doc/install).

You can directly use the `go` tool to download and install the `prometheus`
and `promtool` binaries into your `GOPATH`. We use Go 1.5's experimental
vendoring feature, so you will also need to set the `GO15VENDOREXPERIMENT=1`
environment variable in this case:

    $ GO15VENDOREXPERIMENT=1 go get github.com/prometheus/prometheus/cmd/...
    $ prometheus -config.file=your_config.yml

You can also clone the repository yourself and build using `make`:

    $ mkdir -p $GOPATH/src/github.com/prometheus
    $ cd $GOPATH/src/github.com/prometheus
    $ git clone https://github.com/prometheus/prometheus.git
    $ cd prometheus
    $ make build
    $ ./prometheus -config.file=your_config.yml

The Makefile provides several targets:

  * *build*: build the `prometheus` and `promtool` binaries
  * *test*: run the tests
  * *format*: format the source code
  * *vet*: check the source code for common errors
  * *assets*: rebuild the static assets
  * *docker*: build a docker container for the current `HEAD`

## More information

  * The source code is periodically indexed: [Prometheus Core](http://godoc.org/github.com/prometheus/prometheus).
  * You will find a Travis CI configuration in `.travis.yml`.
  * All of the core developers are accessible via the [Prometheus Developers Mailinglist](https://groups.google.com/forum/?fromgroups#!forum/prometheus-developers) and the `#prometheus` channel on `irc.freenode.net`.

## Contributing

Refer to [CONTRIBUTING.md](https://github.com/prometheus/prometheus/blob/master/CONTRIBUTING.md)

## License

Apache License 2.0, see [LICENSE](https://github.com/prometheus/prometheus/blob/master/LICENSE).


[travis]: https://travis-ci.org/prometheus/prometheus
[hub]: https://hub.docker.com/r/prom/prometheus/
[circleci]: https://circleci.com/gh/prometheus/prometheus
[quay]: https://quay.io/repository/prometheus/prometheus

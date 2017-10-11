top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

.DEFAULT_GOAL := all


THIS_MAKEFILE_DIR=$(shell pwd)
MODULE_NAME=$(shell basename $(THIS_MAKEFILE_DIR))
GO_UTILS_DIR=$(THIS_MAKEFILE_DIR)/../..
ARCH := amd64
GPDB_VERSION := $(shell ../../../../getversion --short)

.NOTPARALLEL:

all : dependencies build

dependencies :
		go get -d github.com/greenplum-db/gpbackup/utils
		go get github.com/cppforlife/go-semi-semantic/version
		go get github.com/onsi/ginkgo/ginkgo
		go get golang.org/x/tools/cmd/goimports
		go get github.com/onsi/gomega
		go get github.com/jessevdk/go-flags
		go get golang.org/x/crypto/ssh
		go get -u github.com/golang/lint/golint
		go get github.com/alecthomas/gometalinter
		go get github.com/golang/protobuf/protoc-gen-go
		go get github.com/spf13/cobra
		go get github.com/pkg/errors
		go get google.golang.org/grpc
		go get
# Counterfeiter is not a proper dependency of the app. It is only used occasionally to generate a test class that
# is then checked in.  At the time of that generation, it can be added back to run the dependency list, temporarily.
#		go get github.com/maxbrunsfeld/counterfeiter

format :
		gofmt -s -w .

lint :
		gometalinter --config=gometalinter.config ./...

unit :
		ginkgo -r -randomizeSuites -randomizeAllSpecs -race --skipPackage=integrations

sshd_build :
		make -C integrations/sshd

integration:
		ginkgo -r -randomizeAllSpecs -race integrations

test : format lint unit integration

protobuf :
		protoc -I idl/ idl/*.proto --go_out=plugins=grpc:idl

build :
		go build -ldflags "-X gp_upgrade/commands.GpdbVersion=$(GPDB_VERSION)" -o $(GO_UTILS_DIR)/bin/$(MODULE_NAME)
		go build -ldflags "-X gp_upgrade/commands.GpdbVersion=$(GPDB_VERSION)" -o $(GO_UTILS_DIR)/bin/command_listener $(MODULE_NAME)/commandListener
		go build -ldflags "-X gp_upgrade/commands.GpdbVersion=$(GPDB_VERSION)" -o $(GO_UTILS_DIR)/bin/command_sample $(MODULE_NAME)/commandSample


coverage: build
		./scripts/run_coverage.sh

linux :
		GOOS=$@ GOARCH=$(ARCH) go build -ldflags "-X gp_upgrade/commands.GpdbVersion=$(GPDB_VERSION)" -o $(GO_UTILS_DIR)/bin/$(MODULE_NAME).$@
darwin :
		GOOS=$@ GOARCH=$(ARCH) go build -ldflags "-X gp_upgrade/commands.GpdbVersion=$(GPDB_VERSION)" -o $(GO_UTILS_DIR)/bin/$(MODULE_NAME).$@

platforms : linux darwin

install : build
	mkdir -p $(prefix)/bin
	cp -p ../../bin/gp_upgrade $(prefix)/bin/

clean:
	rm -f ../../bin/gp_upgrade
	rm -rf /tmp/go-build*
	rm -rf /tmp/ginkgo*

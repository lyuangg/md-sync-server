GO ?= go
OUT ?= dist
TARGETS := darwin-arm64 darwin-amd64 linux-amd64 linux-arm64 windows-amd64

.PHONY: all darwin check clean

all: $(TARGETS:%=build-%)

build-%:
	@os=$(word 1,$(subst -, ,$*)); arch=$(word 2,$(subst -, ,$*)); ext=""; \
	if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
	echo "  build md-sync-server_$${os}_$${arch}$${ext}"; \
	mkdir -p $(OUT) && CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build \
		-trimpath -ldflags "-s -w" \
		-o $(OUT)/md-sync-server_$${os}_$${arch}$${ext} .

darwin: build-darwin-arm64 build-darwin-amd64

check:
	$(GO) vet ./...

clean:
	rm -f $(OUT)/md-sync-server_*

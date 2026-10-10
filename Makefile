SPEC_URL := https://github.com/mcpwarp/ws-mixer-spec.git
GOSERVER_URL := https://github.com/mcpwarp/ws-mixer-go.git
REPO := $(CURDIR)

# fetch-spec shallow-clones ws-mixer-spec at the tag pinned in spec.pin into
# .spec/ (gitignored), the checkout the conformance runner and the fixture
# tests read from. No-op if .spec/ is already at that pin. Same convention as
# ws-mixer-go's Makefile and ws-mixer-js's scripts/fetch-spec.mjs.
.PHONY: fetch-spec
fetch-spec:
	@pin=$$(cat spec.pin); \
	if [ -f .spec/.pin ] && [ "$$(cat .spec/.pin)" = "$$pin" ]; then \
		echo "fetch-spec: .spec/ already at $$pin"; \
	elif { [ -e .spec ] || [ -L .spec ]; } && [ ! -f .spec/.pin ]; then \
		echo "fetch-spec: .spec exists without a .pin stamp; remove it first" >&2; \
		exit 1; \
	else \
		rm -rf .spec; \
		echo "fetch-spec: cloning ws-mixer-spec@$$pin into .spec/"; \
		git clone --depth 1 --branch "$$pin" $(SPEC_URL) .spec; \
		echo "$$pin" > .spec/.pin; \
	fi

# fetch-goserver does the same for ws-mixer-go at goserver.pin into
# .goserver/, the server side of the pair conformance jobs.
.PHONY: fetch-goserver
fetch-goserver:
	@pin=$$(cat goserver.pin); \
	if [ -f .goserver/.pin ] && [ "$$(cat .goserver/.pin)" = "$$pin" ]; then \
		echo "fetch-goserver: .goserver/ already at $$pin"; \
	elif { [ -e .goserver ] || [ -L .goserver ]; } && [ ! -f .goserver/.pin ]; then \
		echo "fetch-goserver: .goserver exists without a .pin stamp; remove it first" >&2; \
		exit 1; \
	else \
		rm -rf .goserver; \
		echo "fetch-goserver: cloning ws-mixer-go@$$pin into .goserver/"; \
		git clone --depth 1 --branch "$$pin" $(GOSERVER_URL) .goserver; \
		echo "$$pin" > .goserver/.pin; \
	fi

.PHONY: adapter
adapter:
	cargo build --release --locked -p ws-mixer-conformance-adapter
	mkdir -p build
	rm -f build/rust-adapter
	cp target/release/conformance-adapter build/rust-adapter

# The conformance build tag compiles in wsmixer.AllowSubfloorTiming, which the
# Go adapter needs.
.PHONY: go-adapter
go-adapter: fetch-goserver
	mkdir -p build
	rm -f build/go-adapter
	cd .goserver && go build -tags conformance -o "$(REPO)/build/go-adapter" ./cmd/conformance-adapter

# .spec is its own Go module, so the runner has to be run from inside it with
# absolute paths pointing back into this repo.
.PHONY: conformance-fixture
conformance-fixture: adapter fetch-spec
	cd .spec && go run ./conformance/runner \
		--spec-root "$(REPO)/.spec" \
		--adapter "rust=$(REPO)/build/rust-adapter" \
		--sdk rust --mode fixture \
		--counts "$(REPO)/conformance/COUNTS.json" \
		--report "$(REPO)/build/conformance-fixture.xml"

# --sdk go,rust runs both go->go and go->rust pairs.
.PHONY: conformance-pair
conformance-pair: adapter go-adapter fetch-spec
	cd .spec && go run ./conformance/runner \
		--spec-root "$(REPO)/.spec" \
		--adapter "go=$(REPO)/build/go-adapter,rust=$(REPO)/build/rust-adapter" \
		--sdk go,rust --mode pair \
		--counts "$(REPO)/conformance/COUNTS.json" \
		--report "$(REPO)/build/conformance-pair.xml"

# The runner reports every row as SKIP (and exits 0) when the adapter never
# prints `ready`, so check that directly.
.PHONY: smoke
smoke: adapter
	@line=$$(echo '{"seq":1,"cmd":"shutdown"}' | build/rust-adapter | head -1); \
	echo "$$line"; \
	echo "$$line" | grep -q '"event":"ready"' || { echo "smoke: adapter did not print ready" >&2; exit 1; }

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: clippy
clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

.PHONY: test
test:
	cargo test

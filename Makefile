# To update rust:
# - rustup default nightly
# - rustup update

SRC = $(wildcard src/*.rs)
SRC_PLUS = $(wildcard src/*.rs) $(wildcard examples/*.rs)

.PHONY: format
format:
	cargo fmt

.PHONY: test
test:
	$(MAKE) format
	cargo clippy --fix --allow-dirty --allow-no-vcs
	env RUST_BACKTRACE=1 ANVIL_TEST_QUICK=true cargo test

.PHONY: presubmit
presubmit:
	$(MAKE) clean
	cargo clippy --tests -- -D warnings
	! grep -r -e "print!" -e "println!" src | grep -v "^src/logging.rs:"
	$(MAKE) format
	cargo clippy --fix --allow-dirty --allow-no-vcs
	env RUST_BACKTRACE=1 cargo test

.PHONY: clean
clean:
	cargo clean
	$(RM) Cargo.lock
	$(RM) -r target
	$(RM) -r bench
	$(RM) perf.data
	$(RM) perf.data.old


# BENCHMARKING RECIPES

NO_FS_BENCH_SLUGS := bench_skip_list_set
NO_FS_BENCHES := $(addprefix bench/,$(patsubst %, %.bench.txt, $(NO_FS_BENCH_SLUGS)))
FS_BENCHES := writing_wal_keys_unformatted \
	bench_wal_write_throughput
FS_BENCHES := $(addprefix bench/,$(patsubst %, %.bench.txt, $(FS_BENCHES)))

bench/%.bench.txt: $(SRC)
	echo "Running benchmark $*"
	mkdir -p bench
	cargo bench $* -- --include-ignored > $@

.PHONY: bench
bench: $(NO_FS_BENCHES)
	@echo "Note: These benchmark do not touch the file system."
	@echo "For a fully featured benchmark suite, run 'make bench-full'"


.PHONY: bench-full
bench-full: $(NO_FS_BENCHES) $(FS_BENCHES)
	@echo "Note: These benchmarks touch the file system."
	@echo "If run excessively, this may cause disk wear."
	@echo "Run 'make bench' for a safer benchmark that uses simulated drive performance."

bench/kv-dump-flamegraph.svg: $(SRC_PLUS)
	@echo 'Consider running "sudo sysctl kernel.perf_event_paranoid=0"'
	cargo flamegraph --example kv_dump -- skip-raw
	mkdir -p bench
	mv flamegraph.svg bench/kv-dump-flamegraph.svg
	@echo "Flamegraph: file://`pwd`/bench/kv-dump-flamegraph.svg"
	$(RM) perf.data
	$(RM) perf.data.old

.PHONY: kv-dump-benchmark
kv-dump-benchmark: $(SRC_PLUS) bench/kv-dump-flamegraph.svg
	cargo run --example kv_dump
	@echo "Flamegraph: file://`pwd`/bench/kv-dump-flamegraph.svg"
	$(RM) perf.data
	$(RM) perf.data.old

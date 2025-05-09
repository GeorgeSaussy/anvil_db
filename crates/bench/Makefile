SRC_PLUS := $(shell find src -type f -name '*.rs') \
	$(shell find examples -type f -name '*.rs') \
	$(shell find ../../src -type f -name '*.rs')

bench/simple-set-flamegraph.svg: $(SRC_PLUS)
	@echo 'Consider running "sudo sysctl kernel.perf_event_paranoid=0"'
	cargo flamegraph --release --example  simple_set
	mkdir -p bench
	mv flamegraph.svg bench/simple-set-flamegraph.svg
	@echo "Flamegraph: file://`pwd`/bench/simple-set-flamegraph.svg"
	$(RM) perf.data
	$(RM) perf.data.old

.PHONY: simple-set
simple-set: $(SRC_PLUS) bench/simple-set-flamegraph.svg
	cargo run --release --example  simple_set
	@echo "Flamegraph: file://`pwd`/bench/simple-set-flamegraph.svg"
	$(RM) perf.data
	$(RM) perf.data.old

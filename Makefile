# To update rust:
# - rustup default nightly
# - rustup update

.PHONY: format
format:
	cargo fmt

.PHONY: test
test:
	$(MAKE) format
	cargo clippy --fix --allow-dirty --allow-no-vcs
	env RUST_BACKTRACE=1 cargo test

.PHONY: run
run:
	cargo run

zz:
.PHONY: presubmit
presubmit:
	$(MAKE) clean
	cargo clippy --tests -- -D warnings
	! grep -r -e "print!" -e "println!" src | grep -v "^src/logging.rs:"
	$(MAKE) test

.PHONY: clean
clean:
	cargo clean
	$(RM) Cargo.lock
	$(RM) -r target

run_distributor:
	@cargo run --manifest-path ./distributer/Cargo.toml

run_processor:
	@cargo run --manifest-path ./processor/Cargo.toml

run:
	@$(MAKE) run_distributor &
	@$(MAKE) run_processor &
	@wait
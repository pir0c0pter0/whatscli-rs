build:
	cargo build --release --locked

clean:
	cargo clean

run:
	cargo run

install:
	cargo install --path . --locked

update:
	cargo update

test:
	cargo test --locked

check:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings
	cargo test --locked

release:
	./release.sh

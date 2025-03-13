FROM espressif/idf-rust:all_latest

# Set the working directory
WORKDIR /workspace

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Install the Rust dependencies
RUN cargo build --release

# Copy the source code
COPY src ./src

# Set the entry point for the container
CMD ["cargo", "run", "--release"]
FROM rust:1.67

RUN git clone https://github.com/toshi-search/Toshi.git
RUN git reset --hard a13a518

COPY ./config.toml ./Toshi/config/config.toml

WORKDIR ./Toshi

RUN cargo build --release
WORKDIR ./
ENTRYPOINT ./target/release/toshi

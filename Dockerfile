FROM rust:1.67

RUN git clone https://github.com/toshi-search/Toshi.git
WORKDIR ./Toshi
RUN git reset --hard a13a518
COPY ./config.toml ./config/config.toml

RUN cargo build --release
WORKDIR ./
ENTRYPOINT ./target/release/toshi

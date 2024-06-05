FROM rust:1.67

# RUN apk update && apk upgrade && apk add git
RUN git clone https://github.com/toshi-search/Toshi.git

WORKDIR ./Toshi

RUN cargo build --release
WORKDIR ./
ENTRYPOINT ./target/release/toshi


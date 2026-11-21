# Velocity v5: native 8-thread pipeline + direct Parquet (no Python/JVM at runtime).
FROM gcc:14-bookworm AS builder
WORKDIR /build
COPY src/main.cpp src/fast_parquet.hpp ./
RUN g++ -O3 -DNDEBUG -std=c++20 -march=x86-64 -flto -pipe -pthread \
    -static-libstdc++ -static-libgcc -o velocity main.cpp

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /build/velocity /app/velocity
COPY run.sh /app/run.sh
RUN chmod +x /app/velocity /app/run.sh
ENV HOME=/scratch VELOCITY_NATIVE_PARQUET=1
WORKDIR /app
CMD ["/app/run.sh"]

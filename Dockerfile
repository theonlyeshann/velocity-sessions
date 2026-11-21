# Portable x86-64 native engine + lightweight DuckDB Parquet writer.
# GCC runtime is linked into the binary to avoid libstdc++ ABI mismatches.
FROM gcc:14-bookworm AS native
WORKDIR /build
COPY src/main.cpp ./main.cpp
RUN g++ -O3 -DNDEBUG -std=c++20 -march=x86-64 -flto -pipe -pthread \
    -static-libstdc++ -static-libgcc -o velocity main.cpp

FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir duckdb==1.4.2 && \
    useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=native /build/velocity /app/velocity
COPY src/write_parquet.py /app/write_parquet.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/velocity
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/scratch
WORKDIR /app
CMD ["/app/run.sh"]

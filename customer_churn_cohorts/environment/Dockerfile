FROM python:3.11-slim

# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends bash curl git && \
    rm -rf /var/lib/apt/lists/*

# Python packages required by dbt + DuckDB + tests
RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-duckdb==1.8.* \
    duckdb==1.1.* \
    pytest

# Working directory expected by solve.sh and tests
WORKDIR /app

# Create directory structure used by the tasks
RUN mkdir -p /app/database \
             /app/dbt_transforms/models/staging \
             /app/dbt_transforms/models/intermediate \
             /app/dbt_transforms/models/marts \
             /app/dbt_transforms/seeds \
             /app/dbt_transforms/macros \
             /solution \
             /tests \
             /logs/verifier

# Default dbt profiles.yml (solve.sh may overwrite this)
RUN echo 'dbt_transforms:\n\
  target: dev\n\
  outputs:\n\
    dev:\n\
      type: duckdb\n\
      path: /app/database/retail.duckdb\n\
      schema: main' > /app/dbt_transforms/profiles.yml

# Default entrypoint
CMD ["bash"]

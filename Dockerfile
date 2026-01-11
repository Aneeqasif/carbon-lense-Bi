ARG MELTANO_IMAGE=meltano/meltano:v3.7.0-python3.11
FROM $MELTANO_IMAGE

WORKDIR /project

# Copy Meltano project
COPY . .

# Install plugins
RUN meltano install

# Create output directory
RUN mkdir -p /project/output/duckdb

# Readonly mode - config changes not allowed
ENV MELTANO_PROJECT_READONLY=1

ENTRYPOINT ["meltano"]
CMD ["--help"]

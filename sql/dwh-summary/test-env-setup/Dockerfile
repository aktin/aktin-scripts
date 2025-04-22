FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

#Install Sudo
RUN apt-get update && apt-get install -y sudo
RUN echo "ALL ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/90-nopasswd

RUN useradd -r -m -U -s /bin/bash postgres

# Install PostgreSQL 12
RUN apt-get update && \
    apt-get install -y wget gnupg2 lsb-release && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql-12 && \
    apt-get clean

# Copy init script into the container
COPY init-db.sql /docker-entrypoint-initdb.d/
COPY insert1.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

# Ensure PostgreSQL service starts correctly with the correct user
CMD service postgresql start && tail -f /dev/null
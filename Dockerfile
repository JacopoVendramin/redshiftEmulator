FROM postgres:16.4

# Copy init SQL scripts (run in alphanumeric order on first startup)
COPY init/ /docker-entrypoint-initdb.d/

# Copy utility scripts to PATH
COPY scripts/ /usr/local/bin/

# Note: If locale/collation issues arise with Alpine, switch to postgres:16 (Debian).

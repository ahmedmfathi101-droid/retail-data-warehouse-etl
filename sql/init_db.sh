#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER dw_user WITH PASSWORD 'dw_pass';
    CREATE DATABASE retail_dw;
    GRANT ALL PRIVILEGES ON DATABASE retail_dw TO dw_user;
    \c retail_dw
    GRANT ALL ON SCHEMA public TO dw_user;
    SET ROLE dw_user;
    \i /tmp/create_tables.sql
EOSQL

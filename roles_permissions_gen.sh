#!/bin/bash
set -e

DB_NAME=$POSTGRES_DB
DB_USER=$POSTGRES_USER
DB_PASS=$POSTGRES_PASSWORD

execute_sql() {
    PGPASSWORD=$DB_PASS psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "$DB_NAME" -c "$1"
}

execute_sql "CREATE ROLE reader;"
execute_sql "CREATE ROLE writer;"
execute_sql "CREATE ROLE fullaccess NOLOGIN;"

execute_sql "GRANT SELECT ON ALL TABLES IN SCHEMA untappd_db TO reader;"
execute_sql "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA untappd_db TO writer;"
execute_sql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA untappd_db TO fullaccess;"
execute_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA untappd_db GRANT SELECT ON TABLES TO reader;"
execute_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA untappd_db GRANT SELECT, INSERT, UPDATE ON TABLES TO writer;"
execute_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA untappd_db GRANT ALL ON TABLES TO fullaccess;"

execute_sql "CREATE USER analytic WITH PASSWORD 'analytic';"
execute_sql "GRANT SELECT ON TABLE untappd_db.beer TO analytic;"

NUM_USERS=${N:-1}

for i in $(seq 1 $NUM_USERS); do
    username="fullaccess_user_$i"
    execute_sql "CREATE USER $username WITH PASSWORD 'password';"
    execute_sql "GRANT fullaccess TO $username;"
done

echo "The reader, writer, fullaccess, analytic user, and $NUM_USERS fullaccess user roles have been successfully created and configured."

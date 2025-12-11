#!/usr/bin/env bash
# wait-for-postgres.sh

set -e

host="$1"
shift
cmd="$@"

# CRITICAL FIX: Changed user from "bank_user" to the administrative user "postgres" 
# with password "postgres" to successfully check the database service readiness.
until PGPASSWORD=postgres psql -h "$host" -U "postgres" -d postgres -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec $cmd
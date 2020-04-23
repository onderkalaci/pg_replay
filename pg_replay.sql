-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgcolor" to load this file. \quit


CREATE SCHEMA replay_internal;
CREATE TABLE replay_internal.replay_targets(conn_str text);

CREATE OR REPLACE FUNCTION pg_catalog.add_replay_target(text)
RETURNS void
AS '$libdir/pg_replay'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_catalog.remove_replay_target(text)
RETURNS void
AS '$libdir/pg_replay'
LANGUAGE C IMMUTABLE STRICT;

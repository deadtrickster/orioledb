/* contrib/orioledb/sql/orioledb--1.10--1.11_prod.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.11'" to load this file. \quit

-- Maintenance entry point, like verify_orioledb() revoked in 1.8.
REVOKE ALL ON FUNCTION orioledb_tbl_check(oid, boolean) FROM PUBLIC;

-- Provision the `tern` database with Apache AGE, configured so every session
-- auto-loads the AGE library and resolves the ag_catalog search_path — no
-- per-query `LOAD 'age'` needed (which is what makes pog ↔ AGE ergonomic).
CREATE DATABASE tern;
\connect tern
CREATE EXTENSION IF NOT EXISTS age;
ALTER DATABASE tern SET session_preload_libraries = 'age';
ALTER DATABASE tern SET search_path = ag_catalog, "$user", public;

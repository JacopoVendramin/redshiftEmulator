-- 00-config.sql: PostgreSQL session/database configuration for Redshift emulation
-- Sets defaults that match Redshift's behavior.

-- Set the default search path (public schema)
ALTER DATABASE dev SET search_path TO public;

-- Standard conforming strings (Redshift default: on)
ALTER DATABASE dev SET standard_conforming_strings = on;

-- Timezone: Redshift default is UTC
ALTER DATABASE dev SET timezone = 'UTC';

-- Extra float digits: Redshift uses 1 (compact float display)
ALTER DATABASE dev SET extra_float_digits = 1;

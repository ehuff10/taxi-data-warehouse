\echo 'Creating schemas (if needed)...'
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

\echo 'Building silver.cleaned_trips_2024_01...'
\i sql/silver_cleaned_trips_2024_01.sql

\echo 'Building gold marts...'
\i sql/build_gold.sql

\echo 'Build complete.'

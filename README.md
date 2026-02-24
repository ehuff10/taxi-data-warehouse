# Taxi Data Warehouse
A layered data warehouse built in PostgreSQL to model, transform, and analyze NYC Yellow Taxi trip data using a structured Bronze → Silver → Gold architecture.

This repository demonstrates production-style data modeling, transformation logic, and analytical mart design for transportation revenue and operational performance analysis.
### Architecture Overview

The warehouse follows a three-layer architecture:

### Bronze Layer — Raw Ingestion

- Source: NYC Yellow Taxi Parquet dataset (January 2024)
- Data loaded without transformation
- Preserves original schema and values
- Row count: 2,964,624 records

**Purpose**
- Maintain source fidelity
- Enable reproducibility
- Serve as the immutable system of record

### Silver Layer — Cleaned & Typed Fact Table

**Table:** `silver.cleaned_trips_2024_01`

**Transformations Applied**
- Explicit type casting (numeric, integer, timestamp)
- NaN handling and null standardization
- Datetime validation (dropoff ≥ pickup)
- Derived trip duration (interval + minutes)
- Primary key (`trip_id`)
- Performance indexes on:
  - `pickup_dt`
  - `pu_location_id`
  - `do_location_id`

**Result**
- 2,964,568 validated trip records
- Analytics-ready fact table at trip-level grain

**Purpose**
- Enforce data quality
- Standardize schema
- Enable reliable downstream aggregations


### Gold Layer — Analytical Marts

#### 1. Daily KPI Mart  
**Table:** `gold.daily_kpis_2024_01`

**Metrics**
- Daily trip volume
- Total revenue
- Average fare and total amount
- Average trip distance
- Average trip duration
- Payment mix (credit vs cash)

**Business Use Cases**
- Revenue trend analysis
- Operational performance monitoring
- Payment behavior analysis


#### 2. Revenue by Pickup Zone  
**Table:** `gold.revenue_by_pickup_zone_2024_01`

**Metrics**
- Trip count by pickup zone
- Total revenue by zone
- Average fare metrics
- Average trip distance and duration

**Business Use Cases**
- Geographic revenue analysis
- Demand concentration modeling
- Zone-level performance evaluation

## Reproducibility

The warehouse can be rebuilt end-to-end using SQL build scripts.

### Build Silver + Gold

```bash
psql "postgresql://taxi:taxi@localhost:5433/taxi_dw" -f sql/build_all.sql
```

This will:
- Create schemas if needed
- Rebuild the Silver fact table
- Rebuild all Gold marts

## Technology Stack

- PostgreSQL 16
- Docker
- Python (Pandas, PyArrow, Psycopg)
- SQL

## Data Modeling Approach

- Fact-based modeling centered on trip-level grain
- Dimensional-style aggregation for Gold marts
- Explicit typing to prevent silent data drift
- Deterministic rebuild scripts for consistency
- Indexed fact table for performance optimization

## Analytical Scope

This warehouse supports:

- Time-series revenue analysis
- Trip efficiency evaluation
- Payment channel distribution tracking
- Geographic demand segmentation
- KPI-driven operational review

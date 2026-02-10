# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dbt project** (`acme_dbt` v1.0.0) implementing a medallion architecture for a newspaper publishing company data warehouse on **Snowflake**. The domain covers readers, subscriptions, content articles, digital engagement (web/mobile), print distribution, and marketing campaigns.

## Common Commands

```bash
# Setup
cp profiles.yml.example ~/.dbt/profiles.yml   # Then fill in Snowflake creds
dbt deps                                       # Install dbt_utils package

# Run models
dbt run                                        # Run all models
dbt run --select staging                       # Run an entire layer
dbt run --select stg_readers                   # Run a single model
dbt run --select staging+                      # Run staging and all downstream

# Test
dbt test                                       # Run all tests
dbt test --select staging                      # Test a specific layer
dbt test --select stg_readers                  # Test a single model

# Debug & docs
dbt debug                                      # Validate connection and config
dbt docs generate && dbt docs serve            # Generate and view docs
dbt source freshness                           # Check source data freshness
```

## Architecture

### Medallion Layers

Each layer writes to its own Snowflake database, suffixed by environment (`_dev` or `_prod` via `DBT_ENV` env var, defaults to `dev`):

| Layer | Database | Materialization | Purpose |
|---|---|---|---|
| **landing** | `landing_{env}` | table | Raw data ingestion from `raw` schema |
| **staging** | `staging_{env}` | view (transient) | Cleaning, type casting, standardization |
| **intermediate** | `intermediate_{env}` | table | Business logic transformations |
| **reporting** | `reporting_{env}` | table | BI-ready dimensional models (not yet built) |

Each layer also has an `incremental/` subdirectory for merge-strategy incremental models.

### Model DAG

```
Sources (landing.raw.*) --> 14 staging models (stg_*) --> 5 intermediate models (int_*) --> reporting (future)
```

**Intermediate models and their upstream staging dependencies:**
- `int_readership_events` - Unifies web, mobile, and print engagement into one event stream
- `int_subscriber_status` - Derives current subscription state with tenure/risk indicators
- `int_content_engagement` - Aggregates article-level engagement metrics
- `int_marketing_touchpoints` - Normalizes marketing sends and interaction events
- `int_print_distribution` - Combines print editions, deliveries, routes, and centers

### Key Macros

- `get_env_database(layer)` - Returns `{layer}_{env}` (e.g., `staging_dev`). Used in model configs.
- `generate_database_name()` - Overrides dbt's default database name resolution; passes through custom database names from layer configs.

## Conventions

### Naming
- Staging: `stg_{source_entity}` (e.g., `stg_readers`, `stg_web_events`)
- Intermediate: `int_{business_concept}` (e.g., `int_subscriber_status`)
- Reporting: `rpt_` prefix (reserved)

### SQL Pattern
All models follow this CTE structure:
```sql
{{ config(alias='model_name') }}

with source as (
    select * from {{ source('raw', 'table_name') }}  -- staging
    -- or {{ ref('stg_...') }}                        -- intermediate
),

cleaned as (  -- or "transformed", "final", etc.
    select
        {{ dbt_utils.generate_surrogate_key(['natural_key']) }} as pk,
        -- fields with casting, trimming, lowercasing
    from source
    where natural_key is not null
)

select * from cleaned
```

### File Organization
- YAML configs at each layer root: `_{layer}__models.yml` (model docs + tests) and `_sources.yml` (landing only)
- SQL models in `{layer}/standard/` subdirectories
- All primary keys get `unique` and `not_null` tests; categoricals get `accepted_values` tests

### Dependencies
- **dbt**: >=1.7.0, <2.0.0
- **dbt_utils**: >=1.0.0, <2.0.0 (surrogate keys, utilities)

### Environment Variables
- `DBT_ENV` - `dev` or `prod` (defaults to `dev`)
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` - required
- `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` - optional (defaults in profiles.yml)

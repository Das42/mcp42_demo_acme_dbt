# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `acme_dbt`, a dbt project modeling a newspaper publishing company on Snowflake. It implements a 4-layer medallion architecture with environment-specific databases.

## Common Commands

```bash
# Parse project (validate syntax)
dbt parse

# Run all models
dbt run

# Run specific layer
dbt run -s tag:staging
dbt run -s tag:intermediate
dbt run -s tag:reporting

# Run single model and its upstream dependencies
dbt run -s +model_name

# Run tests
dbt test
dbt test -s model_name

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

## Environment Configuration

Environment is controlled via `DBT_ENV` variable (defaults to 'dev'):
- Databases follow pattern: `{layer}_{env}` (e.g., `staging_dev`, `reporting_prod`)
- Dev uses `TRANSFORMER_DEV` role, `TRANSFORMING_DEV` warehouse
- Prod uses `TRANSFORMER_PROD` role, `TRANSFORMING_PROD` warehouse

Copy `profiles.yml.example` to `profiles.yml` and configure Snowflake credentials.

## Architecture

### Layer Structure

| Layer | Database | Materialization | Purpose |
|-------|----------|-----------------|---------|
| Landing | `landing_{env}` | Table | Raw source data (defined in `_sources.yml`) |
| Staging | `staging_{env}` | View (transient) | Standardization, cleaning, surrogate keys |
| Intermediate | `intermediate_{env}` | Table | Business logic, dimensional modeling |
| Reporting | `reporting_{env}` | Table | Kimball facts/dimensions for BI |

### Model Directory Structure

```
models/
├── landing/           # Sources only (_sources.yml)
├── staging/standard/  # stg_* models (14 models)
├── intermediate/standard/  # int_* models (5 models)
└── reporting/standard/     # Future fact/dim tables
```

## Naming Conventions

- **Staging models**: `stg_{entity}` (e.g., `stg_readers`, `stg_subscriptions`)
- **Intermediate models**: `int_{concept}` (e.g., `int_readership_events`)
- **Primary keys**: `{entity}_pk` (surrogate)
- **Natural keys**: `{entity}_id`
- **Booleans**: `is_{condition}`

## Model Development Patterns

### Staging Model Template

```sql
{{
   config(
       alias='model_name'
   )
}}

with source as (
    select * from {{ source('raw', 'table_name') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['natural_key']) }} as entity_pk,
        cast(field as type) as field,
        lower(trim(field)) as standardized_field
    from source
    where natural_key is not null
)

select * from cleaned
```

### Intermediate Model Template

```sql
{{
   config(
       alias='model_name'
   )
}}

with staging_data as (
    select * from {{ ref('stg_model') }}
),

transformed as (
    select
        -- columns and business logic
    from staging_data
)

select * from transformed
```

## Testing

Tests are defined in YAML files alongside models:
- All surrogate primary keys require `unique` and `not_null` tests
- Categorical fields use `accepted_values` tests

Example in `_staging__models.yml`:
```yaml
columns:
  - name: reader_pk
    tests:
      - unique
      - not_null
```

## Key Files

- `dbt_project.yml` - Project configuration and materialization defaults
- `packages.yml` - Dependencies (dbt_utils v1.x)
- `macros/get_env_database.sql` - Environment-specific database name helper
- `models/landing/_sources.yml` - All 17 raw source definitions

## Data Domain

The project models newspaper publishing operations:
- Reader subscriptions and churn tracking
- Multi-channel engagement (web, mobile, print)
- Print distribution and delivery
- Marketing campaigns and interactions
- Content/article management

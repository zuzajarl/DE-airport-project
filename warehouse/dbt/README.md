# dbt Layer

This directory contains the dbt transformation layer for the project.

## Current project

- project root: `warehouse/dbt/krk_flights_dbt`

## Model groups

- `models/staging`: normalized source-aligned models
- `models/intermediate`: deduplicated latest-flight snapshots
- `models/marts`: dashboard-facing tables used by Looker Studio

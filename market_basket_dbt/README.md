# Market Basket dbt

dbt models for the market basket project. The project reads the Delta tables
created by `extract/extract_data.py` in the `workspace.projeto_market_basket`
Bronze schema. The dbt default schema naming convention writes Silver models to
`projeto_market_basket_silver` and Gold marts to `projeto_market_basket_gold`
inside the `workspace` catalog.

Staging models are the Silver layer. Invalid rows are written to `rej_*` models
with `rejection_reason` and `rejected_at`; valid staging models keep one row per
natural key. Marts are the Gold layer.

## Run

The Databricks profile is configured in `~/.dbt/profiles.yml` under
`market_basket_dbt` and uses OAuth.

```bash
dbt debug
dbt build
```

Complete the browser OAuth flow when prompted. The Bronze tables must exist
before building the staging and mart models.
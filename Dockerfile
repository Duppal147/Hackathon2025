FROM quay.io/astronomer/astro-runtime:12.7.1

ENV COSMOS_CACHE_DISABLED=1




 # install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-snowflake && pip install --no-cache-dir dbt-postgres && deactivate
    
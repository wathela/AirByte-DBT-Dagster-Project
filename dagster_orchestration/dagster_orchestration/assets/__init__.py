import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# @dbt_assets(manifest=Path("Users/wathelahamed/Documents/Courses/Data_engineering/end-to-end-data-engineering-project-4413618/dbt_transformation/target", "manifest.json"))
# def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], project_dir=os.getenv("DBT_PROJECT_DIR"), profiles_dir=os.getenv("DBT_PROFILES_DIR"), context=context).stream()


# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)

# Define your dbt assets using the `dbt_assets` function
dbt_assets_def = dbt_assets(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
    key_prefix=["transformed_data"]
)



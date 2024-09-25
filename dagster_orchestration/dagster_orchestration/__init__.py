# from dagster import Definitions, load_assets_from_modules
from dagster import Definitions

# Import the resources and dbt_assets from assets
from .assets import resources, dbt_assets_def

# Create Definitions to bundle resources and assets
defs = Definitions(
    assets=[dbt_assets_def],
    resources=resources
)
# defs = Definitions(assets=load_assets_from_modules(
#     [assets]), resources=resources)
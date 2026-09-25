import dagster as dg


@dg.asset
def yt_catalog_initial(context: dg.AssetExecutionContext) -> dg.MaterializeResult: 
    

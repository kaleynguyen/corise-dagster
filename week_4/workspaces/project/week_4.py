from typing import List

from dagster import (
    Nothing,
    String,
    asset,
    AssetIn,
    SourceAsset,
    AssetKey,
    with_resources
)
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset (
    config_schema={"s3_key":String},
    required_resource_keys={"s3"},
    description="Reading data from S3",
    op_tags={"kind":"s3"},
    group_name="corise"
)
def get_s3_data(context):
    # You can reuse the logic from the previous week
    s3_key = context.op_config["s3_key"]
    s3_client = context.resources.s3
    stocks = [Stock.from_list(stock) for stock in s3_client.get_data(s3_key)]
    return stocks



@asset(
    ins={'stocks': AssetIn(key="get_s3_data")},
    description="Retrieve the highest stock",
    group_name="corise"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    # You can reuse the logic from the previous week
    highest_stock = max(stocks, key=lambda x: x.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    required_resource_keys={"redis"},
    description="Put highest stock to redis",
    ins={"stock_agg": AssetIn(key="process_data")},
    op_tags={"kind":"redis"},
    group_name="corise"
)
def put_redis_data(context, stock_agg: Aggregation) -> Nothing:
    # You can reuse the logic from the previous week
    redis_client = context.resources.redis
    redis_client.put_data(name = stock_agg.date.strftime('%Y-%m-%d'),
                          value = str(stock_agg.high))


@asset(
    required_resource_keys={"s3"},
    description="Put highest stock to *the same* s3",
    ins={'stock_agg': AssetIn(key="process_data")},
    op_tags={"kind":"s3"},
    group_name="corise"
)
def put_s3_data(context, stock_agg: Aggregation) -> Nothing:
    # You can reuse the logic from the previous week
    s3_client = context.resources.s3
    s3_client.put_data( key_name = stock_agg.date.strftime("%Y-%m-%d"), 
                        data = stock_agg)
    


get_s3_data_docker, \
process_data_docker, \
put_redis_data_docker, \
put_s3_data_docker \
    = with_resources(
    definitions=[   get_s3_data, 
                    process_data, 
                    put_redis_data, 
                    put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        's3': {
            'config': {
                'bucket': 'dagster',
                'access_key': 'test',
                'secret_key': 'test',
                'endpoint_url': 'http://localhost:4566',
            }
        },
        'redis': {
            'config': {
                'host': 'redis',
                'port': 6379,
            }
        },
    },   
)

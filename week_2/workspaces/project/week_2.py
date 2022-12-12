from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key":String},
    required_resource_keys={"s3"},
    description="Reading data from S3",
    out={'stocks': Out(List[Stock])}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    s3_client = context.resources.s3
    stocks = [Stock.from_list(stock) for stock in s3_client.get_data(s3_key)]
    return stocks
    


@op(
    ins={'stocks': In(List[Stock])},
    out={'stock_agg': Out(Aggregation)},
    description="Retrieve the highest stock"
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key=lambda x: x.high)
    context.log.debug(highest_stock)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    required_resource_keys={"redis"},
    description="Put highest stock onto redis",
    ins={'stock_agg': In(Aggregation)}
)
def put_redis_data(context, stock_agg: Aggregation):
    redis_client = context.resources.redis
    redis_client.put_data(stock_agg.date.isoformat(), str(stock_agg.high))


@op(
    required_resource_keys={"s3"},
    description="Put highest stock onto *the same* s3",
    ins={'stock_agg': In(Aggregation)}
)
def put_s3_data(context, stock_agg: Aggregation):
    s3_client = context.resources.s3
    s3_client.put_data(stock_agg.date.isoformat(), str(stock_agg.high))


@graph
def week_2_pipeline():
    processed_data = process_data(get_s3_data())
    put_redis_data(processed_data)
    put_s3_data(processed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={ "s3": mock_s3_resource,
                    "redis": ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={ "s3": s3_resource,
                    "redis": redis_resource}
)

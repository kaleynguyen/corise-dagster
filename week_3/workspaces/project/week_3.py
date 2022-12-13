from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    String,
    ScheduleEvaluationContext
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key":String},
    required_resource_keys={"s3"},
    description="Reading data from S3",
    out={'stocks': Out(List[Stock])}
)
def get_s3_data(context):
    # You can reuse the logic from the previous week
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
def put_redis_data(context, stock_agg: Aggregation) -> Nothing:
    redis_client = context.resources.redis
    redis_client.put_data(name = stock_agg.date.strftime('%Y-%m-%d'),
                          value = str(stock_agg.high))


@op(
    required_resource_keys={"s3"},
    description="Put highest stock onto *the same* s3",
    ins={'stock_agg': In(Aggregation)}
)
def put_s3_data(context, stock_agg: Aggregation) -> Nothing:
    s3_client = context.resources.s3
    s3_client.put_data( key_name = stock_agg.date.strftime("%Y-%m-%d"), 
                        data = stock_agg)


@graph
def week_3_pipeline():
    # You can reuse the logic from the previous week
    processed_data = process_data(get_s3_data())
    put_redis_data(processed_data)
    put_s3_data(processed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys = [str(i) for i in range(1,11)])
def docker_config(partition_key: str):
    partition_config = docker.copy()
    partition_config['ops']['get_s3_data']['config']['s3_key'] = 'prefix/stock_{}.csv'.format(partition_key)
    return partition_config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)

)


week_3_schedule_local = ScheduleDefinition( job=week_3_pipeline_local,
                                            cron_schedule="*/15 * * * *")

# ref: https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
@schedule(
    job=week_3_pipeline_docker,
    cron_schedule="0 * * * *"
)
def week_3_schedule_docker(context: ScheduleEvaluationContext):
    return RunRequest(  run_key=context.scheduled_execution_time.strftime("%Y-%m-%d %H:%M:%S"),
                        run_config=docker_config()
)


@sensor(
    job=week_3_pipeline_docker,
    minimum_interval_seconds=30,
)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(bucket='dagster',
                            prefix='prefix',
                            endpoint_url='http://localstack:4566/')
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    sensor_docker_config = docker.copy()
    for new_file in new_files:
        sensor_docker_config['ops']['get_s3_data']['config']['s3_key'] = '{}'.format(new_file) 
        yield RunRequest(run_key=new_file,
                         run_config=sensor_docker_config)



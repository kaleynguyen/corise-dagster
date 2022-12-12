import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema = {'s3_key': String},
    description = 'Reading data from S3',
    out = Out(is_required = False, dagster_type = List[Stock]) 
) 
def get_s3_data(context) -> List[Stock]:
    def yielder():
        yield from csv_helper(context.op_config['s3_key'])
    return list(yielder())


@op(
    ins={"data": In(dagster_type=List[Stock],
                    description="List of stocks at given time")},
    out=Out(is_required=True, dagster_type=Aggregation)
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key=attrgetter('high'))
    context.log.debug(highest_stock)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    ins={"highest_value": In(dagster_type=Aggregation, 
                             description="The datetime the highest value was seen and the value itself.")}
)
def put_redis_data(context, highest_value: Aggregation):
    pass

@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))

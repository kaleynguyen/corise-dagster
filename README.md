# co:rise Data Engineering with Dagster

This repo contains the code to complete my co:rise Data Engineering with Dagster course (https://corise.com/course/dagster). Instructions and content is contained within the co:rise course itself.


# Workflow
1. Read data from S3
2. Perform data processing
3. Write result to redis

# Simple Example

## Definition
* op: smallest unit and contains a single unit of work
* graph: contains any number of ops and a graph define the DAG of one or more ops
* job: a graph that is parameterized  

```python
from dagster import graph, op
@op
def get_name():
    return 'dagster'

@op
def capitalize_name(name: str) -> str:
    return name.capitalize()

@op
def hello(name: str) -> str:
    print('hello, {name}'.format(name))

@graph
def hello_dagster():
    hello(get_name())

job = hello_dagster.to_job()
```

## Conditional branching

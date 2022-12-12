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

## Defintion 

Conditional branching is useful where some data is not needed to run through the pipeline can be discarded since each computation is expensive. For example, a list of users that want to opt out of the email newsletter or to protect their data privacy. 

```python
@op(
    config_schema={"name": String},
    out={
        "capitalized": Out(is_required=False),
        "not_capitalized": Out(is_required=False),
    },
)
def get_name(context) -> str:
    name = context.op_config["name"]
    # Determine if name is already capitalized
    if name[0].isupper():
        yield Output(name, "capitalized")
    else:
        yield Output(name, "not_capitalized")

@graph
def hello_dagster():
    capitalized, not_capitalized = get_name()
    hello.alias('hello_needto_cap')(capitalize_name(not_capitalized))
    hello.alias('hello_notneedto_cap')(capitalized)
```

# Dynamic Output

## Definition
DynamicOutput is useful when we need to process a list of data and each element is processed singly and collected at the end. 

```python
@op (out=DynamicOut())
def get_name() -> str:
    for name in ["dagster", "mike", "molly"]:
        yield DynamicOutput(name, mapping_key=name)

@op
def capitalize_name(names: str) -> str:
    return name.capitalize()

@op
def hello(names: list):
    for name in names:
        print(f"Hello, {name}!")


@graph
def hello_dagster():
    names = get_name()
    capitalized_names = names.map(capitalize_name)
    hello(capitalized_names.collect())
```

# Data types and data contracts 
## Definition
Dagster allows both Python data types and custom data types. 

```python
from dagster import DagsterType

DagName = DagsterType(
    type_check_fn=lambda _, value: "dag" in value, #must contain dag
    name="DagName",
    description="A string that must include `dag`"
)

@op
def get_name() -> DagName:
    return 'dagster'

@op(ins={"name": In(dagster_type=DagName, description="Generated dag name")})
def hello(name):
    print(f"Hello, {name}!")
```

```python
from datetime import datetime
from dagster import usable_as_dagster_type
from pydantic import BaseModel, validator


@usable_as_dagster_type(description="A string that must include `dag`")
class PydanticDagName(BaseModel):
    name: str
    date_time: datetime

    @validator("name", allow_reuse=True)
    def name_must_contain_dag(cls, value):
        assert 'dag' in value, 'Name must contain `dag`'
        return value
```

# Architecture
## Definition
* dagit: UI for viewing and interacting with dagster objects
* daemon: long-running processes such as schedulers, sensors, and run queuing 
* workspace: ops, graphs, jobs
* postgres: data artifacts storage 
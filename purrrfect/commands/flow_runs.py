import functools
from typing import Optional, List, Type

import dateparser
import pygum
import typer
from prefect import Flow
from prefect.client.schemas import FlowRun
from prefect.server.schemas.filters import (
    PrefectFilterBaseModel,
    FlowRunFilterStartTime,
    FlowRunFilter,
)
from pytz import tzinfo
import pendulum
from rich.table import Table

from purrrfect.prefect_api import prefect_client
from purrrfect.printer import console
from purrrfect.typer_utils import AsyncTyper


async def get_flow_run(prompt_text: Optional[str] = None):
    flow_runs = await prefect_client.read_flow_runs()

    flow_runs_dict = {flow_run.name: str(flow_run.id) for flow_run in flow_runs}

    return flow_runs_dict[pygum.filter(flow_runs_dict.keys(), prompt=prompt_text)]


async def get_flow_runs(**kwargs) -> List[FlowRun]:
    flow_runs = await prefect_client.read_flow_runs(**kwargs)

    return flow_runs


flow_run_app = AsyncTyper()


@flow_run_app.async_command("get")
async def get_flow_run_cmd():
    chosen_flow_run = await get_flow_run()
    print(chosen_flow_run)


def parse_time_string(time_string: str):
    valid_start_tokens = ["BEFORE", "AFTER", "BETWEEN"]
    start_token = time_string.split(" ")[0].upper()

    if start_token not in valid_start_tokens:
        # We'll assume that the user is trying to use the default which is AFTER
        operation = "AFTER"
    else:
        operation = start_token

    if operation == "BETWEEN":
        time_string = time_string.replace("between", "")
        split_time = time_string.split("and")
        print(split_time)
        if len(split_time) != 2:
            raise ValueError(
                "BETWEEN operation must be followed by two times separated by AND"
            )
        start_time = dateparser.parse(split_time[0].strip())
        end_time = dateparser.parse(split_time[1].strip())
        return (operation, start_time, end_time)
    else:
        parsed_time = dateparser.parse(time_string)
        return (operation, parsed_time, None)


def time_string_typle_to_prefect_time_filter(
    time_tuple, time_filter_class: Type[PrefectFilterBaseModel]
):
    print(time_tuple)
    operation, start_time, end_time = time_tuple
    if operation == "BEFORE":
        return time_filter_class(before_=end_time)
    elif operation == "AFTER":
        return time_filter_class(after_=start_time)
    elif operation == "BETWEEN":
        return time_filter_class(before_=end_time, after_=start_time)


def convert_time(prefect_time):
    return str(
        pendulum.local_timezone()
        .convert(pendulum.instance(prefect_time))
        .to_day_datetime_string()
    )


@flow_run_app.async_command("list")
async def list_flow_runs_cmd(
    start_time: Optional[str] = typer.Option(None, help="Start time to filter by"),
):
    flow_run_filters = {}
    if start_time is not None:
        flow_run_filters["start_time"] = time_string_typle_to_prefect_time_filter(
            parse_time_string(start_time), FlowRunFilterStartTime
        )
    if flow_run_filters:
        flow_run_filters = FlowRunFilter(**flow_run_filters)
        console.print(f"Filtering by {flow_run_filters}")

    flow_runs = await get_flow_runs(flow_run_filter=flow_run_filters)

    flow_runs_table = Table(title="Flow Runs")
    flow_runs_table.add_column("Name")
    flow_runs_table.add_column("Flow Name")
    flow_runs_table.add_column("State")
    flow_runs_table.add_column("Start Time")
    flow_runs_table.add_column("End Time")
    flow_runs_table.add_column("Run Time")
    flow_runs_table.add_column("URL")
    flow_cache = {}
    for flow_run in flow_runs:
        if flow_run.flow_id not in flow_cache:
            flow_cache[flow_run.flow_id] = await prefect_client.read_flow(
                flow_run.flow_id
            )

        flow = flow_cache[flow_run.flow_id]
        flow_runs_table.add_row(
            flow_run.name,
            flow.name,
            f"[bold green]{str(flow_run.state_name)}[/bold green]",
            convert_time(flow_run.start_time),
            convert_time(flow_run.end_time),
            (
                pendulum.instance(flow_run.end_time)
                - pendulum.instance(flow_run.start_time)
            ).in_words(),
            f"https://cloud.prefect.io/flow-run/{flow_run.id}",
            ",".join(flow.tags),
        )
    console.print(flow_runs_table)
    console.print("Visit my [link=https://www.willmcgugan.com]blog[/link]!")


if __name__ == "__main__":
    flow_run_app()

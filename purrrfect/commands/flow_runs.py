import json
import uuid
from typing import Annotated, List, Optional, Protocol, Type

import dateparser
import pendulum
import pygum
import typer
from prefect.client.schemas import FlowRun
from prefect.server.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterName,
    FlowRunFilterStartTime,
    PrefectFilterBaseModel,
)
from rich.console import Console
from rich.table import Table

from purrrfect.configs import OutputEnum
from purrrfect.prefect_api import prefect_client
from purrrfect.printer import console
from purrrfect.typer_utils import AsyncTyper

flow_run_app = AsyncTyper()


class Printer(Protocol):
    output_type: OutputEnum

    async def print(self, *args, **kwargs):
        pass


class ListPrinterProtocol(Printer):
    async def print(self, flow_runs: list[FlowRun]):
        pass


class RichListPrinter(ListPrinterProtocol):
    output_type = OutputEnum.RICH

    async def print(self, flow_runs: list[FlowRun]):
        flow_runs_table = Table(title="Flow Runs")
        flow_runs_table.add_column("id")
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
                str(flow_run.id),
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


class PlainListPrinter(Printer):
    output_type = OutputEnum.PLAIN

    async def print(self, flow_runs):
        for flow_run in flow_runs:
            console.print(
                f"{flow_run.name} - {flow_run.state_name} - {convert_time(flow_run.start_time)}"
            )


class STDOUTListPrinter(Printer):
    output_type = OutputEnum.STDOUT

    async def print(self, flow_runs: list[FlowRun]):
        for flow_run in flow_runs:
            tmp_console = Console(
                no_color=True, force_terminal=True, force_jupyter=False
            )
            console.print(
                f"{flow_run.deployment_id} {flow_run.name} | {flow_run.state_name} | {convert_time(flow_run.start_time)}"
            )


class JsonListPrinter(Printer):
    output_type = OutputEnum.JSON

    async def print(self, flow_runs):
        print(
            json.dumps([flow_run.dict(json_compatible=True) for flow_run in flow_runs])
        )


class QuietListPrinter(Printer):
    output_type = OutputEnum.QUIET

    async def print(self, flow_runs):
        for flow_run in flow_runs:
            console.print(f"{flow_run.id}")


async def get_flow_run(prompt_text: Optional[str] = None):
    flow_runs = await prefect_client.read_flow_runs()

    flow_runs_dict = {flow_run.name: str(flow_run.id) for flow_run in flow_runs}

    return flow_runs_dict[pygum.filter(flow_runs_dict.keys(), prompt=prompt_text)]


@flow_run_app.async_command("get")
async def get_flow_run_cmd(flow_run_id: Optional[str] = None):
    if flow_run_id is None:
        flow_run_id = await get_flow_run()

    print(await prefect_client.read_flow_run(flow_run_id))


async def get_flow_runs(**kwargs) -> List[FlowRun]:
    flow_runs = await prefect_client.read_flow_runs(**kwargs)

    return flow_runs


def parse_time_string(time_string: str):
    valid_start_tokens = ["BEFORE", "AFTER", "BETWEEN"]
    start_token, time_to_parse = time_string.split(" ", maxsplit=1)
    start_token = start_token.upper()

    if start_token not in valid_start_tokens:
        # We'll assume that the user is trying to use the default which is AFTER
        operation = "AFTER"
    else:
        operation = start_token

    if operation == "BETWEEN":
        time_string = time_string.replace("between", "")
        split_time = time_string.split("and")
        if len(split_time) != 2:
            raise ValueError(
                "BETWEEN operation must be followed by two times separated by AND"
            )
        start_time = dateparser.parse(split_time[0].strip())
        end_time = dateparser.parse(split_time[1].strip())
        return (operation, start_time, end_time)
    else:
        parsed_time = dateparser.parse(time_to_parse)
        return (operation, None, parsed_time)


def time_string_tuple_to_prefect_time_filter(
    time_tuple, time_filter_class: Type[PrefectFilterBaseModel]
):
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


def build_flow_run_fitler_args(
    debug,
    start_time,
    name_like,
    names: list[str],
    deployment_id: uuid.UUID,
    no_deployments: bool,
):
    flow_run_filters_args = {}
    if start_time is not None:
        parsed_time = parse_time_string(start_time)

        if debug:
            console.print(f"Time string parsed to {parsed_time}")

        flow_run_filters_args["start_time"] = time_string_tuple_to_prefect_time_filter(
            parsed_time, FlowRunFilterStartTime
        )

    if name_like is not None:
        if debug:
            console.print(f"Filtering by name like {name_like}")
        flow_run_filters_args["name"] = FlowRunFilterName(like_=name_like)

    if names is not None:
        if debug:
            console.print(f"Filtering by names {names}")
        flow_run_filters_args["name"] = FlowRunFilterName(any_=names)

    if deployment_id is not None:
        if debug:
            console.print(f"Filtering by deployment id {deployment_id}")
        flow_run_filters_args["deployment_id"] = FlowRunFilterDeploymentId(
            any_=[deployment_id]
        )

    if no_deployments:
        if debug:
            console.print("Filtering out flow runs with deployments")
        flow_run_filters_args["deployment_id"] = FlowRunFilterDeploymentId(
            is_null_=True
        )

    return flow_run_filters_args


async def print_flow_runs(
    flow_runs: List[FlowRun], output_type: OutputEnum, printers: list[Printer]
):
    await {printer.output_type: printer() for printer in printers}[output_type].print(
        flow_runs
    )


async def list_flow_runs(
    start_time,
    name_like,
    names,
    deployment_id: uuid.UUID | None,
    no_deployments: bool,
    debug,
    output_type,
    quiet,
):
    flow_run_filters_args = build_flow_run_fitler_args(
        debug, start_time, name_like, names, deployment_id, no_deployments
    )

    if quiet:
        output_type = OutputEnum.QUIET

    if flow_run_filters_args:
        filters = FlowRunFilter(**flow_run_filters_args)
        if debug:
            console.print(f"Filtering by {flow_run_filters_args}")
    else:
        filters = None

    flow_runs = await get_flow_runs(flow_run_filter=filters)

    await print_flow_runs(
        flow_runs,
        output_type,
        [
            RichListPrinter,
            PlainListPrinter,
            JsonListPrinter,
            STDOUTListPrinter,
            QuietListPrinter,
        ],
    )


@flow_run_app.async_command("list")
async def list_flow_runs_cmd(
    start_time: Optional[str] = typer.Option(None, help="Start time to filter by"),
    name_like: Optional[str] = typer.Option(None, help="Name to filter by"),
    names: Annotated[
        Optional[List[str]], typer.Option(help="FLow names to use")
    ] = None,
    deployment_id: uuid.UUID = typer.Option(
        None,
        help="Deployment ID to use.",
    ),
    no_deployments: bool = typer.Option(
        False, help="Show only flow runs with no deployment"
    ),
    debug: bool = typer.Option(False, help="Print debug information"),
    output_type: OutputEnum = typer.Option(default=OutputEnum.RICH),
    quiet: bool = typer.Option(
        default=False, help="Only print IDs. Shortcut for --output-type quiet"
    ),
):
    typer.echo(prefect_client.api_url)

    names = names or None
    if names is not None and name_like is not None:
        raise typer.BadParameter("Cannot use both --names and --name-like")

    if deployment_id is not None and no_deployments:
        raise typer.BadParameter(
            "Cannot use both --deployment-id and --no-deployments."
        )
    return await list_flow_runs(
        start_time=start_time,
        name_like=name_like,
        names=names,
        deployment_id=deployment_id,
        no_deployments=no_deployments,
        debug=debug,
        output_type=output_type,
        quiet=quiet,
    )


if __name__ == "__main__":
    flow_run_app()

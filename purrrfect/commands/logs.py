import datetime
import time
from typing import List, Optional

import pydantic
import typer
from prefect.server.schemas.filters import LogFilter, LogFilterFlowRunId
from prefect.server.schemas.states import TERMINAL_STATES, StateType

from purrrfect.commands import CONFIG
from purrrfect.commands.config import FormatOptions
from purrrfect.commands.flow_runs.main import get_flow_run
from purrrfect.prefect_api import prefect_client
from purrrfect.printer import console
from purrrfect.typer_utils import AsyncTyper

FlowId = str

logs_app = AsyncTyper()


class LogLineModel(pydantic.BaseModel):
    id: str
    created: datetime.datetime
    updated: datetime.datetime
    name: str
    level: int
    message: str
    timestamp: datetime.datetime
    flow_run_id: str


def plain_text_logs_printer(logs: List[LogLineModel]):
    for line in logs:
        print(f"{line.timestamp} - {line.level} - {line.message}")


def json_logs_printer(logs: List[LogLineModel]):
    [console.print_json(ll.json()) for ll in logs]


def rich_logs_printer(logs: List[LogLineModel]):
    for line in logs:
        console.print(f"{line.timestamp} - {line.level} - {line.message}")


async def stream_flow_logs(
    flow_run_id: FlowId,
    offset: int = 0,
    chunk_size: int = 200,
    interval: int = 1,
):
    lf = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]))

    results = await prefect_client.read_logs(
        log_filter=lf, offset=offset, limit=chunk_size
    )

    async def check_run_status(flow_run_id, results):
        flow_run = await prefect_client.read_flow_run(flow_run_id)

        if len(results) == 0 and flow_run.state_type in TERMINAL_STATES:
            return False
        else:
            return True

    while True:
        yield results
        offset += chunk_size

        if len(results) == 0:
            sleep_interval = interval
        else:
            sleep_interval = 0
        time.sleep(sleep_interval)
        results = await prefect_client.read_logs(
            log_filter=lf, offset=offset, limit=chunk_size
        )
        keep_running = await check_run_status(flow_run_id, results)

        if not keep_running:
            return


LOG_PRINTER_LOOKUP = {
    FormatOptions.PLAIN: plain_text_logs_printer,
    FormatOptions.JSON: json_logs_printer,
}


async def tail_logs(
    flow_run_id, output_format: FormatOptions = "plain", interval: int = 5
):
    async for logs in stream_flow_logs(flow_run_id, interval=interval):
        LOG_PRINTER_LOOKUP[output_format](logs)


@logs_app.async_command("tail")
async def tail_cmd(
    flow_run_id: Optional[FlowId] = None,
    flow_run_name: Optional[str] = None,
    interval: int = 5,
    output_format: FormatOptions = typer.Option(
        CONFIG.logs.default_text_format or CONFIG.default_text_format
    ),
):
    if flow_run_id is None and flow_run_name is None:
        flow_run_id = await get_flow_run()

    await tail_logs(flow_run_id, output_format, interval=interval)

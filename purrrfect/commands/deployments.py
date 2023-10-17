import webbrowser
from typing import Optional
from urllib.parse import urlparse

import pygum
import pyperclip
import typer
from rich.prompt import Prompt

from purrrfect.commands.logs import tail_logs
from purrrfect.prefect_api import prefect_client
from purrrfect.printer import console
from purrrfect.typer_utils import AsyncTyper
import functools

deployments_app = AsyncTyper()


async def get_deployment(prompt_text: Optional[str] = None):
    deployments = await prefect_client.read_deployments()

    if len(deployments) == 0:
        return None
    deployments_dict = {
        deployment.name: str(deployment.id) for deployment in deployments
    }
    return deployments_dict[pygum.filter(deployments_dict.keys(), prompt=prompt_text)]


@deployments_app.async_command("get")
async def get_deployments_cmd(
    clipboard: bool = typer.Option(False, help="If passed copy output to clipboard"),
    fields: str = typer.Option(
        None, help="Fields to print for deployment. Should be comma deliminated"
    ),
):
    choice = await get_deployment()
    if clipboard:
        pyperclip.copy(choice)

    print(choice)


@deployments_app.async_command("run")
async def run_deployment_cmd(deployment_id: Optional[str] = typer.Option(None)):
    if deployment_id is None:
        deployment_id = await get_deployment()

    flow_run = await prefect_client.create_flow_run_from_deployment(deployment_id)

    console.print(f"Deployment run.\n- (T)ail logs\n- (O)pen in browser.\n- (Q)uit")
    choice = Prompt.ask("Choose one:", choices=["T", "O", "Q"])
    # TODO: Refactor this into a dictionary

    if choice == "T":
        await tail_logs(flow_run.id)
    elif choice == "O":
        url = get_url_from_prefect_api_url(str(prefect_client.api_url))
        webbrowser.open(f"{url}flow-runs/flow-run/{flow_run.id}")
    elif choice == "Q":
        raise typer.Exit()


def get_url_from_prefect_api_url(api_url: str):
    parsed_url = urlparse(api_url)

    _, _, _, account_id, _, workspace_id, *_ = parsed_url.path.split("/")
    return f"{parsed_url.scheme}://app.prefect.cloud/account/{account_id}/workspace/{workspace_id}/"


if __name__ == "__main__":
    deployments_app()

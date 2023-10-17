import typer

from purrrfect.commands.config import config_app
from purrrfect.commands.deployments import deployments_app
from purrrfect.commands.flow_runs.main import flow_run_app
from purrrfect.commands.logs import logs_app
from purrrfect.configs import OutputEnum

main_app = typer.Typer()

main_app.add_typer(logs_app, name="logs")
main_app.add_typer(flow_run_app, name="flow-runs")
main_app.add_typer(deployments_app, name="deployments")
main_app.add_typer(config_app, name="config")


@main_app.callback()
def main_app_callback(ctx: typer.Context):
    ctx.obj = {}


if __name__ == "__main__":
    main_app()

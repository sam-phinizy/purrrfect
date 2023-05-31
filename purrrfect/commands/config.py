import pathlib
from enum import StrEnum

import pydantic
import toml

from purrrfect.typer_utils import AsyncTyper

config_app = AsyncTyper()


class FormatOptions(StrEnum):
    PLAIN = "plain"
    JSON = "json"
    RICH = "rich"


class LogConfigModel(pydantic.BaseModel):
    default_text_format: FormatOptions | None = None


class PurrrfectConfigModel(pydantic.BaseModel):
    default_text_format: FormatOptions = "rich"
    short_link: bool | None = None
    logs: LogConfigModel = LogConfigModel()

    def to_toml(self, file_path: str):
        toml.dump(self.dict(), open(file_path, "w"))


def load_config(file_path: pathlib.Path):
    return PurrrfectConfigModel.parse_obj(toml.load(open(file_path)))


def write_config(file_path: pathlib.Path, config: PurrrfectConfigModel):
    config.to_toml(file_path)


@config_app.command("set")
def set_config_value_cmd(config_name: str, value: str):
    print("config")

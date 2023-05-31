import pathlib

from purrrfect.commands.config import PurrrfectConfigModel, load_config, write_config

HOME = pathlib.Path().home()

CONFIG_DIR = HOME / ".purrrfect"

CONFIG_DIR.mkdir(exist_ok=True, parents=True)
CONFIG_FILE = CONFIG_DIR / "config.toml"

if CONFIG_FILE.exists():
    CONFIG = load_config(CONFIG_FILE)
else:
    CONFIG = PurrrfectConfigModel()
    write_config(CONFIG_FILE, CONFIG)

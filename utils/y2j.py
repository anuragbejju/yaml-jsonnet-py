from typing import Any
from ruamel.yaml import YAML
from utils.parser import JsonnetRenderer
import yaml as setting_yaml

settings = setting_yaml.safe_load(open(r"settings.yaml"))

import warnings

warnings.filterwarnings("ignore")


def convert_yaml(
    yaml_data: str, output: Any, array=True, inject_comments=False
) -> None:
    yaml = YAML(typ="rt")
    yaml.version = "1.1"  # type: ignore  # yaml.version is mis-typed as None
    events = yaml.parse(yaml_data)
    output = JsonnetRenderer(events, output, array, inject_comments).render()
    return output

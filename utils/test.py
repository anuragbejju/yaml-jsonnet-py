import deepdiff
from json import loads, dumps
from collections import OrderedDict

from pathlib import Path
from bson import json_util

import importlib, re
import pandas as pd
import datetime
import json
import glob2

import yaml as setting_yaml

settings = setting_yaml.safe_load(open(r"settings.yaml"))


def match_file(file):

    output_path = settings["output_path"] + Path(file).stem + ".jsonnet"
    _jsonnet = importlib.import_module("_jsonnet")
    jsonnet_str = _jsonnet.evaluate_file(output_path)
    obj = json.loads(jsonnet_str)

    with open(settings["ground_path"] + Path(file).stem + ".json", "r") as outfile:
        ground = json.load(outfile, object_hook=json_util.object_hook)
    ground_truth = json.loads(json.dumps(ground, default=str).replace("+00:00", ""))

    if "x-defaults" in ground_truth.keys():
        ground_truth["x-defaults"][0] = ""
        obj["x-defaults"][0] = ""

    if "defaults" in ground_truth.keys():
        ground_truth["defaults"][0] = ""
        obj["defaults"][0] = ""

    diff = deepdiff.DeepDiff(obj, ground_truth)

    print(Path(file).stem)
    print(diff)
    print()

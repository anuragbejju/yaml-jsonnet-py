from pathlib import Path
from tqdm import tqdm
import sys, glob2, copy, re

from ruamel.yaml import YAML
import utils.y2j as y2j

from io import StringIO
from pandas.core.common import flatten

import importlib  # import _jsonnet dynamically because I haven't tested it on Windows yet and they'll need to use the unofficial binary https://pypi.org/project/jsonnet-binary/
import json
from benedict import benedict
import pandas as pd
import collections

import yaml as setting_yaml

settings = setting_yaml.safe_load(open(r"settings.yaml"))


def flatten(x):
    """Flatten comments"""
    if isinstance(x, collections.Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]


def get_associated_key_for_comment(all_key_df, line, file_id, line_start, col_start):
    """Returns the associated key for a comment"""
    pad = len(line) - len(line.lstrip())

    return all_key_df[
        (~all_key_df.raw_key_id.str.contains("<<"))
        & (all_key_df.key_file_id == file_id)
        & (all_key_df.key_line_start >= line_start)
    ].iloc[0]


def get_pad(line):
    """Number of white spaces at the beginning of the sentence"""
    return len(line) - len(line.lstrip())


def get_associated_key(all_key_df, line, file_id, line_start, col_start):
    """Get the associated key for the  merge element"""
    pad = len(line) - len(line.lstrip())
    return all_key_df[
        (all_key_df.key_file_id == file_id)
        & (all_key_df.key_line_start < line_start)
        & (all_key_df.key_col_start < pad)
    ].iloc[-1]


def clean_comments(document):
    """Cleans comments within the yml file"""

    regex1 = re.compile("\s+\- \#")
    fix = "\n".join(
        [
            " " * get_pad(l) + "- null"
            if bool(re.match(regex1, l))
            else l.replace("HH:mm:ss", "'HH:mm:ss'")
            for l in document.splitlines()
        ]
    )

    return fix


def convert_yaml_to_jsonnet(file, initialize=settings["initialize"]):

    # Initialize variable
    all_key_df = []
    all_alias_df = []
    f = StringIO()

    # Set file name
    file_name = Path(file).stem

    read_file = open(file, "r").read().replace("infinity", repr("infinity"))
    read_file = clean_comments(read_file)
    yaml = YAML(typ="rt")
    yaml.version = "1.1"  # type: ignore  # yaml.version is mis-typed as None

    # Parse and get events from the yml file
    # These events are Scalar Event, Sequence Start/ End event, Mapping event etc.

    events = yaml.parse(read_file)
    events_df = (
        pd.DataFrame(events, columns=["event"])
        .reset_index()
        .rename({"index": "event_id"}, axis=1)
    )
    post_processed_read_file = copy.deepcopy(read_file).splitlines()

    output, keys, alias = y2j.convert_yaml(
        yaml_data=read_file, output=f, array=False, inject_comments=False
    )
    output = output[: int(len(output) / 2)]
    output[-1] = output[-1].replace(",\n", "")
    output = [item for item in output if item != "#insert_comment"]
    copy_output = copy.deepcopy(output)
    output = "".join(output).splitlines()

    # Get all keys from the YML file
    # eg:  { geo_K10_: "geo"} the key would be geo and is the 10th in position

    all_key_df = pd.DataFrame(keys, columns=["raw_key_id", "key_event"])
    all_key_df["key_id"] = all_key_df.raw_key_id.apply(
        lambda x: x.replace("[", "").replace("]", "").replace("'", "")
    )
    all_key_df[
        ["key_value", "key_line_start", "key_col_start", "key_line_end", "key_col_end"]
    ] = pd.DataFrame(
        all_key_df.key_event.apply(
            lambda c: [
                c.value,
                c.start_mark.line,
                c.start_mark.column,
                c.end_mark.line,
                c.end_mark.column,
            ]
        ).to_list()
    )
    all_key_df["key_file_id"] = Path(file).stem
    all_key_df["line"] = all_key_df.key_line_start.apply(
        lambda x: post_processed_read_file[x]
    )

    # Get all alias from the YML file
    # eg: default: { <<: *geo} the alias would be *geo

    all_alias_df = pd.DataFrame(alias, columns=["event"])
    all_alias_df[
        ["value", "line_start", "col_start", "line_end", "col_end"]
    ] = pd.DataFrame(
        all_alias_df.event.apply(
            lambda c: [
                c.anchor,
                c.start_mark.line,
                c.start_mark.column,
                c.end_mark.line,
                c.end_mark.column,
            ]
        ).to_list()
    )
    all_alias_df["file_id"] = Path(file).stem
    all_alias_df["line"] = all_alias_df.line_start.apply(
        lambda x: post_processed_read_file[x]
    )
    all_alias_df["key"] = all_alias_df.line.apply(lambda x: x.lstrip().split()[0])

    # Convert jsonnet to json and get path for each keys

    _jsonnet = importlib.import_module("_jsonnet")
    jsonnet_str = _jsonnet.evaluate_snippet("default", "\n".join(output))
    obj = json.loads(jsonnet_str)
    d = benedict(obj, keypath_separator="|")
    k = d.keypaths(indexes=True)

    # Associate path to key
    # eg: default: { geo: &geo} (where path to geo is default.geo )
    path_df = pd.DataFrame(k, columns=["path"])
    path_df["key_id"] = path_df.path.apply(lambda x: x.split("|")[-1])

    # Create a dataframe that links the reference tag to the actual key
    # eg: geo: &geo (where geo is the reference variable)

    refer_by = []
    for line_id, line in enumerate(post_processed_read_file):
        if len(re.findall(r": &\S*|- &\S*", line)) > 0:
            pad = len(line) - len(line.lstrip())
            refer_by.append([line, line_id, pad, re.findall(r": &\S*|- &\S*", line)])

    refer_by_df = pd.DataFrame(
        refer_by, columns=["line", "ref_line_start", "ref_col_start", "reference"]
    ).explode("reference")
    refer_by_df["reference"] = refer_by_df.reference.apply(
        lambda x: re.sub("[^._|A-Za-z0-9/-]+", "", x)
    )
    refer_by_df["ref_file_id"] = Path(file).stem
    refer_by_df["list_flag"] = refer_by_df.reference.apply(
        lambda x: True if x.lstrip().startswith("-") else False
    )
    refer_by_df["reference"] = refer_by_df.reference.apply(
        lambda x: x[1:] if x.lstrip().startswith("-") else x
    )

    original_read_file = read_file.splitlines()

    # Get all alais whivh have to be merged to the associated key
    # eg: default: { <<: *geo} -> geo needs to be merged to default

    indirect_ref = all_alias_df[all_alias_df.key.isin(["<<:"])]
    indirect_ref_associated_key_df = indirect_ref.apply(
        lambda x: get_associated_key(
            all_key_df,
            original_read_file[x.line_start],
            x.file_id,
            x.line_start,
            x.col_start,
        ),
        axis=1,
    )
    indirect_ref_key_reference_df = pd.concat(
        [indirect_ref, indirect_ref_associated_key_df], axis=1
    )

    for line_start in indirect_ref_key_reference_df.line_start.unique():
        post_processed_read_file[line_start] = ""

    f = StringIO()
    read_file = "\n".join(post_processed_read_file)

    processed_output, processed_keys, processed_alias = y2j.convert_yaml(
        yaml_data=read_file, output=f, array=False, inject_comments=False
    )
    processed_output = processed_output[: int(len(processed_output) / 2)]
    processed_output[-1] = processed_output[-1].replace(",\n", "")
    processed_output = [item for item in processed_output if item != "#insert_comment"]
    processed_output = "".join(processed_output)

    # Initialize the reference paths for each key

    if file_name == "default":
        if initialize:
            default_reference_key = pd.DataFrame()
        else:
            default_reference_key = pd.read_csv(settings["reference_file"])
    else:
        default_reference_key = pd.read_csv(settings["reference_file"])

    if len(refer_by_df) > 0:
        if refer_by_df.list_flag.any():
            reference_key_list = refer_by_df[refer_by_df.list_flag].apply(
                lambda x: get_associated_key(
                    all_key_df,
                    original_read_file[x.ref_line_start],
                    x.ref_file_id,
                    x.ref_line_start,
                    x.ref_col_start,
                ),
                axis=1,
            )
            reference_key_list = pd.concat(
                [
                    refer_by_df[refer_by_df.list_flag],
                    reference_key_list[
                        [
                            "key_id",
                            "key_line_start",
                            "key_col_start",
                            "key_col_end",
                            "line",
                        ]
                    ],
                ],
                axis=1,
            ).reset_index(drop=True)

            reference_key_list["key_id"] = reference_key_list.apply(
                lambda x: x.key_id + "[" + str(x.name) + "]", axis=1
            )

        reference_key_no_list = pd.merge(
            refer_by_df[~refer_by_df.list_flag],
            all_key_df[
                ["key_id", "key_line_start", "key_col_start", "key_col_end", "line"]
            ],
            on="line",
        )

        if refer_by_df.list_flag.any():
            reference_key = pd.concat(
                [
                    reference_key_no_list[
                        ["reference", "ref_file_id", "list_flag", "key_id"]
                    ],
                    reference_key_list[
                        ["reference", "ref_file_id", "list_flag", "key_id"]
                    ],
                ]
            ).reset_index(drop=True)
        else:
            reference_key = reference_key_no_list[
                ["reference", "ref_file_id", "list_flag", "key_id"]
            ]

        reference_key = pd.merge(reference_key, path_df, on="key_id")
        reference_key["reference_path"] = reference_key.apply(
            lambda x: (
                x.ref_file_id
                + "."
                + ".".join(
                    [
                        "[" + repr(p.replace("_h_", "-")) + "]" if "_h_" in p else p
                        for p in x.path.split("|")
                    ]
                )
            ).replace(".[", "["),
            axis=1,
        )
        default_reference_key = pd.concat(
            [reference_key[["reference", "reference_path"]], default_reference_key]
        )

        if file_name == "default":
            if initialize:
                default_reference_key.to_csv(settings["reference_file"])

    direct_df = all_alias_df[all_alias_df.key != "<<:"]
    indirect_df = all_alias_df[all_alias_df.key == "<<:"]

    indirect_df["key_id"] = indirect_df.apply(
        lambda x: get_associated_key(
            all_key_df,
            original_read_file[x.line_start],
            x.file_id,
            x.line_start,
            x.col_start,
        ),
        axis=1,
    )["key_id"]

    processed_output_list = processed_output.splitlines()
    for idx, val in indirect_df[["key_id", "value"]].iterrows():
        l_id = [
            line_id
            for line_id, e in enumerate(processed_output_list)
            if val.key_id in e
        ][0]
        ob = processed_output_list[l_id].split(":")
        if "*" in processed_output_list[l_id]:
            processed_output_list[l_id] = (
                (ob[0] + ": " + repr("*" + val.value) + " + " + "".join(ob[1:]))
                .replace("null", "")
                .replace(".[", "[")
            )
        else:
            processed_output_list[l_id] = (
                (ob[0] + ": " + repr("*" + val.value) + " " + "".join(ob[1:]))
                .replace("null", "")
                .replace(".[", "[")
            )

    # Associate comment to its associated key

    comments_df = (
        pd.DataFrame(
            [e for e in flatten(events_df.event.apply(lambda x: x.comment)) if e],
            columns=["event"],
        )
        .reset_index()
        .rename({"index": "event_id"}, axis=1)
    )
    comments_df[
        ["comment", "line_start", "col_start", "line_end", "col_end"]
    ] = pd.DataFrame(
        comments_df.event.apply(
            lambda c: [
                c.value,
                c.start_mark.line,
                c.start_mark.column,
                c.end_mark.line,
                c.end_mark.column,
            ]
        ).to_list()
    )
    comments_df["file_id"] = file_name

    associatied_comments_df = comments_df.apply(
        lambda x: get_associated_key_for_comment(
            all_key_df,
            original_read_file[x.line_start],
            x.file_id,
            x.line_start,
            x.col_start,
        ),
        axis=1,
    )

    comments_df = pd.concat([comments_df, associatied_comments_df], axis=1)
    comments_df = comments_df[
        comments_df.comment.apply(lambda x: not bool(re.match("^\n+$", x)))
    ]

    for idx, val in comments_df[["comment", "raw_key_id"]].iterrows():
        l_id = [
            line_id
            for line_id, e in enumerate(processed_output_list)
            if val.raw_key_id in e
        ][0]
        comment_line = (
            "".join(
                [
                    "\n// " + line.lstrip()[1:].lstrip()
                    for line in val.comment.splitlines()
                    if len(line) > 0
                ]
            )
            + "\n"
        )
        processed_output_list[l_id] = comment_line + processed_output_list[l_id]

    final_op = "\n".join(processed_output_list)

    # Perform clean up operations

    for idx, val in default_reference_key.iterrows():
        final_op = final_op.replace(repr("*" + val.reference), val.reference_path)

    for f in re.findall("_K(.+?)_", final_op):
        final_op = final_op.replace("_K" + f + "_", "")
    for f in re.findall("_M(.+?)_", final_op):
        final_op = final_op.replace("_M" + f + "_", "")
    final_op = final_op.replace("_h_", "-").replace("// \n", "")
    final_op_lines = final_op.splitlines()

    for line_id, line in enumerate(final_op_lines):
        if "['<<']" in line:
            final_op_lines[line_id] = line.replace("['<<']:", "").rstrip()[:-1] + " + {"
            final_op_lines[line_id - 1] = ""

    # Create reference local variables

    if file_name != "default":
        final_op_lines = [
            "local default = import 'default.jsonnet';",
            "{",
            "local " + file_name + " = $ ,",
        ] + final_op_lines[1:]
    else:
        final_op_lines = ["{", "local " + file_name + " = $ ,"] + final_op_lines[1:]

    # Format lists within the jsonnet

    complete_jsonnet = ""
    for line_id, line in enumerate(final_op_lines):
        if (
            (":" not in line)
            & ("//" not in line)
            & ("{" not in line)
            & ("}" not in line)
            & (len(line) > 0)
            & ("=" not in line)
            & ("[" not in line)
            & ("]" not in line)
            & (".yml" not in line)
            & ("|||" not in line)
            & (line.rstrip().endswith(","))
        ):
            final_op_lines[line_id - 1] = final_op_lines[line_id - 1].replace("\n", "")

        else:
            final_op_lines[line_id] = final_op_lines[line_id] + "\n"

    complete_jsonnet = "".join(final_op_lines)
    complete_jsonnet = complete_jsonnet.replace("\n\n//", "\n//")
    complete_jsonnet = complete_jsonnet.replace("\n\n|||", "\n|||")

    # Write the output to jsonnet file

    with open(settings["output_path"] + file_name + ".jsonnet", "w") as f:
        f.write(complete_jsonnet)
    print(file_name + " has been processed!")

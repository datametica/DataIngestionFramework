# Copyright 2023 Datametica Solutions Pvt. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
This module contains functions to flatten and explode JSON objects.
"""
__all__ = ["explode", "flatten", "flatsplode"]

import itertools

LIST_TYPES = (list, tuple)


def flatten_json(ele):
    """
    Flatten JSON object with list values.
    Args:
        ele: Object to flatten
    Returns:
        Flattened JSON object
    """
    out = {}
    for key, value in ele.items():
        if type(value) == dict:
            parent = key
            for k, v in value.items():
                out[parent + "_" + k] = v

    return out


def explode(item):
    """
    Explode JSON object with list values.

    :param dict item: Object to explode

    :Example:

    >>> explode({'fizz': ['buzz', 'jazz', 'fuzz']})
    """
    # Collect item values that are lists/tuples
    lists = (
        [(k, x) for x in v] if any(v) else [(k, None)]
        for k, v in item.items()
        if isinstance(v, LIST_TYPES)
    )
    # Calculate combinations of values in each list
    combos = map(dict, itertools.product(*lists))
    # Yield each combination
    for combo in combos:
        xitem = item.copy()
        xitem.update(combo)
        yield xitem


def flatsplode(item, join="_"):
    """
    Explode & flatten JSON object with list values.

    :param join:
    :param dict item: Object to explode

    :Example:

    >>> flatsplode({'fizz': [{'key': 'buzz'}, {'key': 'jazz'}]})
    """
    for expl in explode(item):
        flat = flatten(expl, join)
        items = filter(lambda x: isinstance(x, LIST_TYPES), flat.values())
        for item in items:
            yield from flatsplode(flat, join)
            break
        else:
            yield flat


def flatten(item, join="."):
    """
    Flattens nested JSON object.

    :param dict item: Object to flatten

    :Example:

    >>> flatten({'fizz': {'buzz': {'jazz': 'fuzz'}}})
    """
    return dict(iterkv(item, (), join))


def iterkv(item, parents=(), join="."):
    """
    Iterate over key/values of item recursively.

    :param dict item: Item to flatten
    :param tuple parents: Running tuple of parent keys
    """
    for key, val in item.items():
        path = parents + (key,)  # Assemble path parts
        key = str.join(join, path)  # join path parts

        # Recurse into nested dict
        if isinstance(val, dict) and any(val):
            yield from iterkv(val, path, join)

        # Or `None` if empty dict
        elif isinstance(val, dict):
            yield (key, None)

        # Otherwise, yield base case
        else:
            yield (key, val)

import importlib
from functools import reduce
from collections import defaultdict
from pyspark.sql.types import StructType, ArrayType, StructField


def _insert_field_into_tree(tree, field):
    parent_field, _, child_fields = field.partition(".")
    tree[parent_field].append(child_fields) if child_fields else tree[parent_field]
    return tree


def _select_fields(schema, fields):
    fields_tree = reduce(_insert_field_into_tree, fields, defaultdict(list))
    new_schema = []
    for parent_field in schema:
        if parent_field.name in fields_tree:
            child_fields = fields_tree[parent_field.name]
            if child_fields:
                data_type = parent_field.dataType.typeName()
                if data_type == "array":
                    child_schema = parent_field.dataType.elementType
                    selected = ArrayType(_select_fields(child_schema, child_fields))
                elif data_type == "struct":
                    child_schema = parent_field.dataType
                    selected = _select_fields(child_schema, child_fields)
                parent_field = StructField(parent_field.name, selected, True)
            new_schema.append(parent_field)
    return StructType(new_schema)


def _drop_field(schema, field):
    if field.count(".") == 0:
        return StructType([x for x in schema if x.name != field])
    else:
        parent_field, _, child_fields = field.partition(".")
        if schema[parent_field].dataType.typeName() == "array":
            child_schema = schema[parent_field].dataType.elementType
            new_child_schema = ArrayType(_drop_field(child_schema, child_fields))
        else:
            child_schema = schema[parent_field].dataType
            new_child_schema = _drop_field(child_schema, child_fields)

        schema[parent_field].dataType = new_child_schema
        return schema


def _filter_fields(schema, select_fields: list = [], drop_fields: list = []):
    if select_fields:
        schema = _select_fields(schema, select_fields)

    if drop_fields:
        for field in drop_fields:
            schema = _drop_field(schema, field)

    return schema


def get_schema(
    source: str,
    name: str,
    select_fields: list = [],
    drop_fields: list = [],
):
    try:
        module = importlib.import_module(f"tools.schemas.{source}")
    except ModuleNotFoundError as e:
        # TODO: list available sources (via inspection)
        raise e

    try:
        schema = getattr(module, name)
    except AttributeError as e:
        # TODO: list available schemas in that module (via inspection)
        raise e

    return _filter_fields(schema, select_fields, drop_fields)

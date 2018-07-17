import types
import inspect
import collections
import sqlalchemy as sa
import marshmallow as mm
import decimal as dec
import datetime as dt

from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy_utils import get_declarative_base
from marshmallow_jsonschema import JSONSchema


########################################################################################################################


class _JSONSchema(JSONSchema):
    """Customized behavior for Nested fields."""

    def _get_schema_for_field(self, obj, field):
        schema = super()._get_schema_for_field(obj, field)
        fkey = getattr(field, 'foreign_class', None)
        if fkey:
            schema['idtype'] = fkey()
        return schema

    def _from_nested_schema(self, obj, field):
        name = field.name
        follow = obj.context.get('_follow', [])

        if name in follow or name in self.context:
            result = super()._from_nested_schema(obj, field)
            result.pop('many', None)
            return result

        return {
            '$ref': f'{field.related_class}#/definitions/{field.related_class}',
            'type': 'object'
        }


########################################################################################################################


class ViewSchema(mm.Schema):
    """Schema for requesting data in JSON format."""
    only = mm.fields.List(mm.fields.String(), attribute='_only', default=None)
    exclude = mm.fields.Method('build_exclude', attribute='_exclude')
    _follow = mm.fields.List(mm.fields.String(), attribute='_follow', default=list)

    def build_exclude(self, original):
        schema = self.context.get('_exclude_rels', None)
        if schema:
            only = original.get('_only', [])
            exclude = original.get('_exclude', [])
            follow = original.get('_follow', [])

            for key, field in schema._declared_fields.items():
                if key not in exclude and isinstance(field, mm.fields.Nested) and hasattr(field, 'related_class'):
                    if key in only or key in follow or key in original:
                        continue
                    else:
                        exclude.append(key)
            return exclude

        orig = original.get('_exclude', [])
        return [orig] if isinstance(orig, str) else orig

    @mm.post_dump(pass_original=True)
    def final(self, data, original):
        data = dict(data)
        original = dict(original)

        result = {
            'only': data.pop('only'),
            'exclude': data.pop('exclude'),
            'context': {
                **data,
                **{k: v for k, v in original.items() if k not in ('_only', '_exclude') and k not in data}
            }
        }
        return result


########################################################################################################################


def column_default(col):
    if col.default:
        if col.default.is_callable:
            return lambda: col.default.arg({})
        else:
            return col.default.arg


def get_class_from_tablename(base, table):
    for c in base._decl_class_registry.values():
        if hasattr(c, '__tablename__') and c.__tablename__ == table:
            return c
    return None


def _column_to_field(cls, col, name, opts):
    """Build a Field from a SQLAlchemy Column."""
    info = col.info

    if 'field' in info:
        field_type = info['field']
    elif 'field' in opts:
        field_type = opts['field']
    else:
        field_type = FIELD_MAP.get(col.type.python_type, mm.fields.Raw)

    if isinstance(field_type, (mm.fields.Field, type(None))):
        return field_type

    options = {
        'required': not bool(col.nullable),
        'allow_none': bool(col.nullable),
        'dump_only': bool(col.primary_key),
        'missing': column_default(col)
    }

    options.update(opts)
    options.update(info)

    field = field_type(**options)

    foreign_keys = list(col.foreign_keys)
    if len(foreign_keys) == 1:
        fkey = foreign_keys[0]
        table_name = fkey.target_fullname.split('.')[0]
        decl_base = get_declarative_base(cls)

        def foreign_class():
            class_ = get_class_from_tablename(decl_base, table_name)
            return class_.__name__ if class_ else 'unknown'

        field.foreign_class = foreign_class

    elif len(foreign_keys) > 1:
        raise ValueError('Multiple foreign keys are not supported.')

    return field


def _hybrid_to_field(cls, hybrid, name, opts):
    """Create a Field from a SQLAlchemy hybrid property."""
    info = hybrid.info
    field_type = info.get('field', None) or opts.get('field', None) or mm.fields.Raw
    if isinstance(field_type, (type(None), mm.fields.Field)):
        return field_type

    options = {
        'dump_only': hybrid.fset is None
    }

    options.update(opts)
    options.update(info)

    return field_type(**opts)


def _relationship_to_field(cls, rel, name, opts):
    """Create a Field from a SQLAlchemy relationship attribute."""
    options = dict(opts)
    options.update(rel.info)

    if rel.uselist:
        options.update(many=True)

    rel_class = rel.argument().__name__
    field = mm.fields.Nested(rel_class, **options)  # Schema has the same name as the class
    field.related_class = rel_class

    return field


########################################################################################################################


FIELD_MAP = {
    dict: mm.fields.Dict,
    list: mm.fields.List,
    str: mm.fields.String,
    int: mm.fields.Integer,
    float: mm.fields.Float,
    dec.Decimal: mm.fields.Decimal,
    bool: mm.fields.Boolean,
    dt.datetime: mm.fields.DateTime,
    dt.date: mm.fields.Date,
    sa.Column: _column_to_field,
    hybrid_property: _hybrid_to_field,
    sa.orm.RelationshipProperty: _relationship_to_field
}


########################################################################################################################


def _make_field(cls, attr, name, opts):
    """Create a Field for a given attribute."""
    field_opts = {k: v for k, v in opts.items() if k != 'field'} if isinstance(opts, dict) else {}

    # Shortcut cases where opts is None, an instance of a Field, or a Field subclass
    if opts is None:
        return None

    elif isinstance(opts, mm.fields.Field):
        return opts

    elif inspect.isclass(opts) and issubclass(opts, mm.fields.Field):
        field_type = opts

    elif 'field' in opts:
        field_type = opts['field']

    else:
        field_type = FIELD_MAP.get(type(attr), None)

    if field_type is None:
        return None
    elif inspect.isfunction(field_type):
        return field_type(cls, attr, name, field_opts)
    else:
        return field_type(**field_opts)


########################################################################################################################


class JsonMetaMixin:
    """A mixin for the database model metaclass that automatically generates a marshmallow schema when the class is
    created."""

    def __init__(cls, name, bases, dict_):
        super().__init__(name, bases, dict_)
        schema_args = getattr(cls, '__schema_args__', {})

        fields = {}
        for attr_name, attr in dict_.items():
            if attr_name.startswith('_'):
                continue

            field_args = schema_args.get(attr_name, {})
            field = _make_field(cls, attr, attr_name, field_args)
            if field:
                fields[attr_name] = field

        BaseSchema = getattr(cls, '__schema__', mm.Schema)
        cls.__schema__ = type(f'{cls.__name__}', (BaseSchema,), fields)


########################################################################################################################


class JsonMixin:
    """Adds behaviors like serializing an object to JSON, updating from JSON, and getting schema information."""

    def to_json(self, schema_attr='__schema__', **kwargs):
        """Serialize the model."""
        schema_cls = getattr(self, schema_attr)
        params = ViewSchema(context={'_exclude_rels': schema_cls}).dump(kwargs).data
        schema = schema_cls(**params)
        return schema.dump(self).data

    @classmethod
    def json_schema(cls, schema_attr='__schema__', **kwargs):
        """Return a JSON schema for the model's schema."""
        schema_cls = getattr(cls, schema_attr)
        params = ViewSchema().dump(kwargs).data
        return _JSONSchema().dump(schema_cls(**params)).data

    @classmethod
    def validate(cls, data, partial=False):
        """Validate data against the classes' schema."""
        schema = cls.__schema__()
        return schema.validate(data, partial=partial)

    @classmethod
    def from_json(cls, data):
        """Creates an object from a JSON document."""
        obj = cls()
        obj.update(data)
        return obj

    def update(self, *args, **kwargs):
        """Update a model's attributes using a JSON document."""
        if len(args) == 1:
            if isinstance(args[0], collections.Mapping):
                data = args[0]
            else:
                raise TypeError('Argument must be an instance of collections.Mapping')
        elif len(args) > 1:
            raise ValueError('update() can only accept a single key-value mapping as a positional argument.')
        else:
            data = kwargs

        loaded = self.__schema__().load(data).data
        extra = {k: v for k, v in data.items() if k not in loaded}

        for key, value in loaded.items():
            setattr(self, key, value)

        return extra


########################################################################################################################


class JsonBaseMeta(JsonMetaMixin, DeclarativeMeta):
    pass


JsonBase = declarative_base(cls=JsonMixin, metaclass=JsonBaseMeta)

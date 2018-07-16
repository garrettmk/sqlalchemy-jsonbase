import inspect
import collections
import sqlalchemy as sa
import marshmallow as mm
import decimal as dec
import datetime as dt

from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy_utils import get_class_by_table, get_declarative_base
from marshmallow_jsonschema import JSONSchema


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


class NestedSchemaPatch:
    def __get__(self, instance, owner):
        rel_schema = instance.related_class.__schema__
        instance.nested = rel_schema
        params = ViewSchema(context={'_exclude_rels': rel_schema}).dump({}).data
        instance.exclude = params['exclude']
        return instance.nested


mm.fields.Nested.nested = NestedSchemaPatch()


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


def _column_prop_to_field(cls, prop, name, opts):
    """Build a Field from a SQLAlchemy Column."""
    col = prop.columns[0]
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

    foreign_keys = list(col.foreign_keys)
    if len(foreign_keys) == 1:
        fkey = foreign_keys[0]
        table_name = fkey.target_fullname.split('.')[0]
        decl_base = get_declarative_base(cls)
        class_ = get_class_from_tablename(decl_base, table_name)
        class_name = class_.__name__ if class_ else 'unknown'
        options.update(foreign_key_class=class_name)
    elif len(foreign_keys) > 1:
        raise ValueError('Multiple foreign keys are not supported.')

    options.update(opts)
    options.update(info)

    field = field_type(**options)
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

    field = mm.fields.Nested(mm.Schema(), **options)
    field.related_class = rel.argument()
    del field.nested

    return field


def _instr_attr_to_field(cls, attr, name, opts):
    field_type = FIELD_MAP.get(type(attr.prop), mm.fields.Raw)

    if field_type is None:
        return None

    elif inspect.isclass(field_type) and issubclass(field_type, mm.fields.Field):
        info = attr.info
        options = dict(opts)
        options.update(info)
        return field_type(**options)

    elif inspect.isfunction(field_type):
        return field_type(cls, attr.prop, name, opts)

    else:
        raise TypeError(f'Unknown field type: {field_type}')


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
    sa.orm.attributes.InstrumentedAttribute: _instr_attr_to_field,
    sa.orm.ColumnProperty: _column_prop_to_field,
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
        params = ViewSchema(context={'_exclude_rels': schema_cls}).dump(kwargs).data
        return JSONSchema().dump(schema_cls(**params)).data

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


JsonBase = declarative_base(cls=JsonMixin)


def all_subclasses(cls):
    return cls.__subclasses__() + [g for s in cls.__subclasses__() for g in all_subclasses(s)]


@sa.event.listens_for(sa.orm.mapper, 'after_configured')
def build_schemas():
    models = all_subclasses(JsonMixin)
    for model in models:
        schema_args = getattr(model, '__schema_args__', {})

        fields = {}
        for attr_name, attr in model.__dict__.items():
            if attr_name.startswith('_'):
                continue

            field_args = schema_args.get(attr_name, {})
            field = _make_field(model, attr, attr_name, field_args)
            if field:
                fields[attr_name] = field

        BaseSchema = getattr(model, '__schema__', mm.Schema)
        model.__schema__ = type(f'{model.__name__}', (BaseSchema,), fields)
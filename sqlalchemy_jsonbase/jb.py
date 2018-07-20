import types
import inspect
import collections
import sqlalchemy as sa
import marshmallow as mm
import marshmallow_jsonschema as mmjs
import decimal as dec
import datetime as dt

from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy_utils import get_declarative_base


########################################################################################################################


old_get_schema = mmjs.JSONSchema._get_schema_for_field
old_from_nested = mmjs.JSONSchema._from_nested_schema


def fix_refs(root_schema, uri_prefix=''):
    """Convert foreign $refs to local, and local to foreign, based on the contents of the root_schema's definitions."""
    root_name = root_schema['$ref'].split('/')[-1]
    definitions = root_schema['definitions'].keys()

    def schema_uri(class_name):
        if class_name in definitions or class_name == root_name:
            return f'#/definitions/{class_name}'
        else:
            return f'{uri_prefix}{class_name}#/definitions/{class_name}'

    def _fix(doc):
        for key, value in doc.items():
            if key == '$ref':
                doc[key] = schema_uri(value.split('/')[-1])
            elif isinstance(value, dict):
                doc[key] = _fix(value)
        return doc

    for name, schema in root_schema['definitions'].items():
        _fix(schema)

    return root_schema


def _get_schema_for_field(self, obj, field):
    """Patches JSONSchema's _get_schema_for_field method."""
    schema = old_get_schema(self, obj, field)
    fkey = getattr(field, 'foreign_class', None)
    if fkey:
        schema['idtype'] = fkey()

    return schema


def _from_nested_schema(self, obj, field):
    """Patches JSONSchema's _from_nested_schema method."""

    follow = self.context.get('_follow', [])
    rel_class = getattr(field, 'related_class', None)
    nested_ctx = self.context.get(field.name, {})
    nested_params = ViewSchema().load(nested_ctx).data

    if field.name in follow or field.name in self.context:
        if isinstance(field.nested, str):
            nested = mm.class_registry.get_class(field.nested)
        else:
            nested = field.nested

        name = nested.__name__
        outer_name = obj.__class__.__name__
        only = nested_params['only']
        exclude = nested_params['exclude']

        # If this is not a schema we've seen, and it's not this schema,
        # put it in our list of schema defs
        if name not in self._nested_schema_classes and name != outer_name:
            wrapped_nested = mmjs.JSONSchema(nested=True)
            wrapped_dumped = wrapped_nested.dump(
                nested(only=only, exclude=exclude)
            )
            self._nested_schema_classes[name] = wrapped_dumped.data
            self._nested_schema_classes.update(
                wrapped_nested._nested_schema_classes
            )

        # and the schema is just a reference to the def
        schema = {
            'type': 'object',
            '$ref': '#/definitions/{}'.format(name)
        }
    else:
        schema = {
            '$ref': f'{rel_class}#/definitions/{rel_class}',
            'type': 'object'
        }

    # NOTE: doubled up to maintain backwards compatibility
    metadata = field.metadata.get('metadata', {})
    metadata.update(field.metadata)

    for md_key, md_val in metadata.items():
        if md_key == 'metadata':
            continue
        schema[md_key] = md_val

    if field.many:
        schema = {
            'type': ["array"] if field.required else ['array', 'null'],
            'items': schema,
        }

    return schema


mmjs.JSONSchema._get_schema_for_field = _get_schema_for_field
mmjs.JSONSchema._from_nested_schema = _from_nested_schema


########################################################################################################################


class ViewSchema(mm.Schema):
    """Schema for requesting data in JSON format."""
    _only = mm.fields.List(mm.fields.String(), missing=None)
    _exclude = mm.fields.Method('build_exclude', missing=list)
    _follow = mm.fields.List(mm.fields.String(), missing=list)

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

    @mm.post_load(pass_original=True)
    def final(self, data, original):
        data = dict(data)
        original = dict(original)

        result = {
            'only': data.pop('_only'),
            'exclude': data.pop('_exclude'),
            'context': {
                **data,
                **{k: v for k, v in original.items() if k not in ('_only', '_exclude') and k not in data}
            }
        }
        return result


########################################################################################################################


def column_default(col):
    """Return the default value for a column."""
    if col.default:
        if col.default.is_callable:
            return lambda: col.default.arg({})
        else:
            return col.default.arg


def get_class_from_tablename(base, table):
    """Return the class mapped to a particular table name, or None."""
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


def _serialize_relationship(self, value, attr, obj):
    """Patches mm.fields.Nested's _serialize method. Used on Nested fields that correspond to SQLAlchemy relationships.
    """
    if isinstance(value, sa.orm.Query):
        value = value.all()

    attr_ctx = self.context.get(attr, {})
    related_class = mm.class_registry.get_class(self.related_class)
    params = ViewSchema(context={'_exclude_rels': related_class}).load(attr_ctx).data
    self.only = params['only']
    self.exclude = params['exclude']
    self.schema.context = attr_ctx

    return mm.fields.Nested._serialize(self, value, attr, obj)


def _deepcopy_patch(self, memo):
    """Patches __deepcopy__ on Nested fields."""
    new = super(mm.fields.Nested, self).__deepcopy__(memo)
    new._serialize = types.MethodType(_serialize_relationship, new)
    return new


def _relationship_to_field(cls, rel, name, opts):
    """Create a Field from a SQLAlchemy relationship attribute."""
    options = dict(opts)
    options.update(rel.info)

    if rel.uselist:
        options.update(many=True)

    rel_class = rel.argument.arg
    field = mm.fields.Nested(rel_class, **options)  # Schema has the same name as the class
    field.related_class = rel_class
    field._serialize = types.MethodType(_serialize_relationship, field)
    field.__deepcopy__ = types.MethodType(_deepcopy_patch, field)

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
        params = ViewSchema(context={'_exclude_rels': schema_cls}).load(kwargs).data
        schema = schema_cls(**params)
        return schema.dump(self).data

    @classmethod
    def json_schema(cls, schema_attr='__schema__', **kwargs):
        """Return a JSON schema for the model's schema."""
        schema_cls = getattr(cls, schema_attr)
        params = ViewSchema().load(kwargs).data
        js_schema = mmjs.JSONSchema(context=params['context']).dump(schema_cls()).data
        return fix_refs(js_schema)

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

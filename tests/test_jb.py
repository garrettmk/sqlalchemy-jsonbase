import pytest
import sqlalchemy as sa
import marshmallow as mm

from jb.jb import Base, column_default, _column_to_field, ViewSchema
from unittest.mock import Mock


########################################################################################################################
# Models


class Model(Base):
    """A test model class."""
    __tablename__ = 'model'

    id = sa.Column(sa.Integer, primary_key=True)
    first = sa.Column(sa.Integer)
    second = sa.Column(sa.String)
    third = sa.Column(sa.Integer)

    __schema_args__ = {
        'first': mm.fields.URL,
        'second': mm.fields.Float(required=True),
        'third': {
            'field': mm.fields.Float,
            'title': 'Third Field',
        }
    }


########################################################################################################################
# Fixtures


@pytest.fixture(scope='session')
def engine():
    eng = sa.create_engine('sqlite:///:memory:')
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture(scope='function')
def session(engine):
    """Creates a new database session for the test."""
    Session = sa.orm.sessionmaker(bind=engine)
    return Session()


########################################################################################################################
# Tests


@pytest.mark.parametrize('kwargs,expected', [
    ({}, {'only': None, 'exclude': [], 'context': {'_follow': []}}),
    ({'_only': 'one'}, {'only': ['one'], 'exclude': [], 'context': {'_follow': []}}),
    ({'_only': ['one', 'two'], '_exclude': 'one'}, {'only': ['one', 'two'], 'exclude': ['one'], 'context': {'_follow': []}}),
    ({'_exclude': ['one', 'two'], '_follow': 'three'}, {'only': None, 'exclude': ['one', 'two'], 'context': {'_follow': ['three']}}),
])
def test_view_schema(kwargs, expected):
    results = ViewSchema().dump(kwargs).data
    assert results == expected


def test_column_default_none():
    col = sa.Column(sa.Integer)
    assert column_default(col) is None


def test_column_default_scalar():
    col = sa.Column(sa.Integer, default=5)
    assert column_default(col) is 5


def test_column_default_func():
    col = sa.Column(sa.Integer, default=lambda: 5)
    func = column_default(col)
    assert callable(func)
    assert func() == 5


@pytest.mark.parametrize('name,col', [
    ('foo', sa.Column(sa.Integer)),
    ('two', sa.Column(sa.String, nullable=False)),
    ('three', sa.Column(sa.Float, primary_key=True)),
    ('four', sa.Column(sa.Text, default='hello')),
    ('five', sa.Column(sa.String, default=lambda: 'world'))
])
def test_column_to_field(name, col):
    field = _column_to_field(name, col, {})

    assert field.required != col.nullable
    assert field.allow_none == col.nullable
    assert field.dump_only == col.primary_key

    default = column_default(col)
    if callable(default):
        assert field.missing() == default()
    else:
        assert field.missing == default


@pytest.mark.parametrize('col,opts', [
    (sa.Column(sa.Integer), {'field': None}),
    (sa.Column(sa.Integer), {'field': mm.fields.String()}),
    (sa.Column(sa.Integer), {'field': mm.fields.String}),
    (sa.Column(sa.Integer), {'required': True}),
])
def test_column_to_field_custom_opts(col, opts):
    field = _column_to_field('name', col, opts)

    if 'field' in opts:
        custom_field = opts.get('field', None)
        if custom_field is None:
            assert field is None
            return
        elif isinstance(custom_field, mm.fields.Field):
            assert field is custom_field
            return
        elif issubclass(custom_field, mm.fields.Field):
            assert isinstance(field, custom_field)

    field_kwargs = ('default', 'attribute', 'data_key', 'validate', 'required',
                    'allow_none', 'load_only', 'dump_only', 'missing', 'error_messages')

    for kwarg in field_kwargs:
        if kwarg in opts:
            assert getattr(field, kwarg) == opts[kwarg]

    for opt in opts:
        if opt not in field_kwargs:
            assert opt in field.metadata
            assert field.metadata[opt] == opts[opt]


@pytest.mark.parametrize('col', [
    (sa.Column(sa.Integer, info={'field': None})),
    (sa.Column(sa.Integer, info={'field': mm.fields.String()})),
    (sa.Column(sa.Integer, info={'field': mm.fields.String})),
    (sa.Column(sa.Integer, info={'required': True, 'title': 'Hello World!'}))
])
def test_column_to_field_custom_info(col):
    info = dict(col.info)

    if 'field' in info:
        custom_field = g

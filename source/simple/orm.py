#! /usr/bin/python3.8
# -*- coding: utf-8 -*-

from datetime import datetime
from threading import RLock


class Field(object):
	def __init__(self, _bind_type, _column_type, _allow_null, _default, _index=False, _primary=False):
		super(object, self).__init__()
		self.field = None
		self.bind_type = _bind_type
		self.column_type = _column_type
		self.default = _default
		self.primary = _primary
		if _primary:
			self.allow_null = False
			self.index = False
		else:
			self.allow_null = _allow_null
			self.index = _index

	def __str__(self):
		return '<{}:{}>'.format(self.field, self.column_type)


class String(Field):
	def __init__(self, _size=36, _allow_null=True, _default=None, _index=False, _primary=False):
		_column_type = 'VARCHAR({})'.format(_size)
		super().__init__(str, _column_type, _allow_null, _default, _index, _primary)


class Integer(Field):
	""" -2147483648 ~ 2147483647, 0 ~ 4294967295 """

	def __init__(self, _unsigned=False, _default=0, _index=False, _size=-1):
		if _size < 1:
			_size = 10 if _unsigned else 11  # use default database value
		_column_type = ('INT({}) UNSIGNED' if _unsigned else 'INT({})').format(_size)
		super().__init__(int, _column_type, False, _default, _index)


class TinyInteger(Field):
	""" -128 ~ 127, 0 ~ 255 """

	def __init__(self, _unsigned=False, _default=0, _index=False, _size=-1):
		if _size < 1:
			_size = 3 if _unsigned else 4  # use default database value
		_column_type = ('TINYINT({}) UNSIGNED' if _unsigned else 'TINYINT({})').format(_size)
		super().__init__(int, _column_type, False, _default, _index)


class SmallInteger(Field):
	""" -32768 ~ 32767, 0 ~ 65535 """

	def __init__(self, _unsigned=False, _default=0, _index=False, _size=-1):
		if _size < 1:
			_size = 5 if _unsigned else 6  # use default database value
		_column_type = ('SMALLINT({}) UNSIGNED' if _unsigned else 'SMALLINT({})').format(_size)
		super().__init__(int, _column_type, False, _default, _index)


class MediumInteger(Field):
	""" -8388608 ~ 8388607, 0 ~ 16777215 """

	def __init__(self, _unsigned=False, _default=0, _index=False, _size=-1):
		if _size < 1:
			_size = 8 if _unsigned else 9  # use default database value
		_column_type = ('MEDIUMINT({}) UNSIGNED' if _unsigned else 'MEDIUMINT({})').format(_size)
		super().__init__(int, _column_type, False, _default, _index)


class Double(Field):
	def __init__(self, _default=0.0, _index=False):
		super().__init__(float, 'DOUBLE', False, _default, _index)


class Microsecond(MediumInteger):
	""" 0 ~ 999999 """

	def __init__(self, _index=False):
		super().__init__(True, 0, _index)


class DateTime(Field):
	def __init__(self, _allow_null=True, _index=False):
		super().__init__(datetime, 'DATETIME', _allow_null, None, _index)


class Json(Field):
	def __init__(self, _allow_null=True, _default=None):
		# super().__init__(dict, 'JSON', _allow_null, _default)
		# we choose LONGTEXT instead of JSON, so we must ensure the content is really a json in program
		super().__init__(dict, 'LONGTEXT', _allow_null, _default)


def build_field_from_desc_table(_field: str, _type: str, _null: str, _key: str, _default, _extra):
	""" the arguments are associated with DESC `TABLE` """
	_type = _type.upper()
	if _type.startswith('INT') or _type.startswith('BIGINT'):
		_bind_type = int
	elif _type.startswith('TINYINT') or _type.startswith('SMALLINT') or _type.startswith('MEDIUMINT'):
		_bind_type = int
	elif _type.startswith('VARCHAR') or _type.startswith('LONGTEXT'):
		_bind_type = str
	elif _type.startswith('DATETIME'):
		_bind_type = datetime
	elif _type.startswith('DOUBLE') or _type.startswith('FLOAT'):
		_bind_type = float
	else:
		_bind_type = None
	if _default is not None:
		if (_bind_type is int) or (_bind_type is float):
			_default = _bind_type(_default)
	_allow_null = (_null == 'YES')
	_index = False
	_primary = False
	if _key == 'PRI':
		_primary = True
	elif _key == 'MUL':
		_index = True
	_field_obj = Field(_bind_type, _type, _allow_null, _default, _index, _primary)
	_field_obj.field = _field
	return _field_obj


class _ModelMetaClass(type):
	def __new__(mcs, _name, _bases, _attrs):
		if _name != 'Model':
			# get table
			database = _attrs.pop('database', None)
			table = _attrs.pop('table', None)
			if not table:
				table = _name.upper()
			if database:
				access_table = '`{}`.`{}`'.format(database, table)
			else:
				access_table = '`{}`'.format(table)
			# get all fields
			primary = None
			_columns = list()
			_indexes = list()
			fields = dict()
			# check all fields, ensure primary unique, and get columns' limitation
			for _k, _v in _attrs.items():
				if isinstance(_v, Field):
					_v.field = _k
					if _v.primary:
						if primary:
							return BaseException('duplicate primary keys: {}, {}'.format(primary, _k))
						primary = _k
						_columns.append('`{}` {} PRIMARY KEY'.format(_k, _v.column_type))
					else:
						if _v.allow_null:
							if _v.default is None:
								_columns.append('`{}` {} DEFAULT NULL'.format(_k, _v.column_type))
							else:
								_columns.append('`{}` {} DEFAULT {}'.format(_k, _v.column_type, _v.default))
						elif _v.default is None:
							_columns.append('`{}` {} NOT NULL'.format(_k, _v.column_type))
						else:
							_columns.append('`{}` {} NOT NULL DEFAULT {}'.format(_k, _v.column_type, _v.default))
						if _v.index:
							_indexes.append('INDEX(`{}`)'.format(_k))
					fields[_k] = _v
			# change attributes
			for _k in fields.keys():
				_attrs[_k] = '`{}`'.format(_k)
			# add table and primary
			if _indexes:
				limitations = '{},{}'.format(','.join(_columns), ','.join(_indexes))
			else:
				limitations = ','.join(_columns)
			_attrs['database'] = database
			_attrs['table'] = table
			_attrs['access_table'] = access_table
			_attrs['lock'] = RLock()  # mutex for operate archive tables
			_attrs['primary'] = '`{}`'.format(primary) if primary else None
			_attrs['limitations'] = limitations
			_attrs['fields'] = fields
		# rebuild class
		return super(_ModelMetaClass, mcs).__new__(mcs, _name, _bases, _attrs)


class Model(dict, metaclass=_ModelMetaClass):
	def __init__(self, _seq=None, **_kw):
		super().__init__(_seq if _seq else _kw)

	@staticmethod
	def archive_table(_table, _archive_datetime: datetime):
		# get archive table format as your need
		return '`{}_{}`'.format(_table.table, _archive_datetime.strftime('%Y%m'))

	def ensure_key_valid(self, _k):
		assert _k in self.fields

	def __str__(self):
		return self.table

	def __setitem__(self, _k, _v):
		self.ensure_key_valid(_k)  # comment this line for release
		return super().__setitem__(_k, _v)

	def __getitem__(self, _k):
		_v = super().__getitem__(_k)
		if _v is not None:
			return _v
		_v = self.fields.get(_k)
		return None if _v is None else _v.default

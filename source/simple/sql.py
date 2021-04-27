#! /usr/bin/python3.8
# -*- coding: utf-8 -*-

from .orm import Model

array = (list, tuple, set)


class Sql(object):

	def __init__(self):
		super(Sql, self).__init__()

	@staticmethod
	def create_table(_table: Model):
		return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(_table.access_table, _table.limitations)

	@staticmethod
	def drop_table(_table: Model):
		return 'DROP TABLE IF EXISTS {}'.format(_table.access_table)

	@staticmethod
	def desc_table(_table: Model):
		return 'DESC {}'.format(_table.access_table)

	@staticmethod
	def insert(_table: Model, _keys: array):
		Sql.ensure_keys_valid(_table, _keys)  # comment this line for release
		return 'INSERT INTO {}({}) VALUES({})'.format(_table.access_table, ','.join(_keys), ','.join('?' * len(_keys)))

	@staticmethod
	def select(_table: Model, _keys: (*array, str), _condition=None, _extra=None):
		Sql.ensure_keys_valid(_table, _keys)  # comment this line for release
		if not isinstance(_keys, str):
			_keys = ','.join(_keys)
		_sql = 'SELECT {} FROM {}'.format(_keys, _table.access_table)
		if _condition:
			_sql += ' WHERE ' + _condition
		if _extra:
			_sql += ' ' + _extra
		return _sql

	@staticmethod
	def select_union(_tables: array, _keys: (*array, str), _condition=None, _extra=None):
		Sql.ensure_tables_keys_valid(_tables, _keys)  # comment this line for release
		if not isinstance(_keys, str):
			_keys = ','.join(_keys)
		_sql_f = 'SELECT {} FROM'.format(_keys) + ' {}'
		if _condition:
			_sql_f += ' WHERE ' + _condition
		_sql_list = [_sql_f.format(_table) for _table in _tables]
		_sql = ') UNION ('.join(_sql_list)
		# if _extra is not None, and include 'ORDER BY XXX', then XXX must in _keys, if not, we will get an error sql
		return '({}) {}'.format(_sql, _extra) if _extra else '({})'.format(_sql)

	@staticmethod
	def update(_table: Model, _keys: array, _condition: str):
		Sql.ensure_keys_valid(_table, _keys)  # comment this line for release
		return "UPDATE {} SET {}=? WHERE {}".format(_table.access_table, '=?,'.join(_keys), _condition)

	@staticmethod
	def replace(_table: Model, _keys: array):
		Sql.ensure_keys_valid(_table, _keys)  # comment this line for release
		return 'REPLACE INTO {}({}) VALUES({})'.format(_table.access_table, ','.join(_keys), ','.join('?' * len(_keys)))

	@staticmethod
	def replace_from_another_table(_table_dst: Model, _table_src: Model, _condition: str):
		return 'REPLACE INTO {} SELECT * FROM {} WHERE {}'.format(
			_table_dst.access_table, _table_src.access_table, _condition)

	@staticmethod
	def delete(_table: Model, _condition: str):
		return 'DELETE FROM {} WHERE {}'.format(_table.access_table, _condition)

	@staticmethod
	def ensure_keys_valid(_table: Model, _keys: (*array, str)):
		if isinstance(_keys, array):
			for _k in _keys:
				_table.ensure_key_valid(_k)
		elif isinstance(_keys, str):
			_key = _keys.strip('\t\r\n ').upper()
			if ('COUNT(0)' == _key) or ('COUNT(*)' == _key):
				return
			elif _key.startswith('COUNT(') and _key.endswith(')'):
				_key = _key[6:-1].strip('`')
			else:
				_key = _key.strip('`')
			_table.ensure_key_valid(_key)

	@staticmethod
	def ensure_tables_keys_valid(_tables: array, _keys: (*array, str)):
		assert len(_tables) > 1
		for _table in _tables:
			assert isinstance(_table, Model)
			Sql.ensure_keys_valid(_table, _keys)

#! /usr/bin/python3.8
# -*- coding: utf-8 -*-

import source.simple.orm as orm
from source.simple.sql import Sql


class InformationSchemaTables(orm.Model):
	""" this table is an import system table in database, we can only query information from it """
	database = 'information_schema'
	table = 'TABLES'

	TABLE_SCHEMA = orm.String(_size=64)
	TABLE_NAME = orm.String(_size=64)
	TABLE_TYPE = orm.String(_size=64)


class ExampleTable(orm.Model):
	UID = orm.String(_size=36, _primary=True)
	NAME = orm.String(_size=256, _allow_null=False, _index=True)

	TM_REQUEST = orm.DateTime(_allow_null=False, _index=True)
	TM_REQUEST_US = orm.Microsecond(_index=True)

	RESULT_DESC = orm.String(_size=1024, _allow_null=True)
	OPTIONS = orm.Json(_allow_null=True)
	ORIGINAL = orm.Json(_allow_null=True)


if __name__ == '__main__':
	_example = ExampleTable()
	print(Sql.create_table(_example))
	print()
	print(Sql.desc_table(_example))

	print()
	print(Sql.drop_table(_example))

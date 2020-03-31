"""
Library to create sessions and make queries to the Database

It makes use of the lib asyncpg
"""

import logging
from itertools import chain
from collections import defaultdict
import asyncpg


class DBSession:
    '''To wrap a asyncpg connection'''

    def __init__(self, uri):
        '''Init a new DB Sessions
        @param uri: PostgreSQL connection URI'''
        self.connection_uri = uri
        self.connection = None

    async def __aenter__(self):
        '''To use into a with statement'''
        self.connection = await asyncpg.connect(self.connection_uri)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        '''Run after the session is used'''
        if exc_type is not None:
            logging.error('An error ocurred: %r : %r : %r', exc_type, exc_value, traceback)
        await self.connection.close()

    async def find_or_create(self, tname: str, rdata: dict, return_field=None):
        '''Make the query and use to the first result to udpate the data
        If it does not exist it will be created
        @param tname: name of the table
        @param rdata: row/object, the data here will be used to make the query
        @return return_field value or None'''
        if not rdata:
            raise RuntimeError('Empty data')
        query, arguments = self._generate_query(tname, rdata)
        query += ' LIMIT 1'
        row = await self.connection.fetchrow(query, *arguments)
        if not row:
            return await self.insert_one(tname, rdata, return_field=return_field)
        rdata.update(dict(row))
        return rdata.get(return_field)

    async def insert(self, tname: str, columns: str, *values, return_field=None, on_conflict=None):
        '''SQL INSERT Query
        columns: str (ie: name, age)
        *values: tuples (ie: ('Nano', 33))'''
        query = 'INSERT INTO {} ({}) VALUES {}'.format(
            tname, columns, self._insert_values_query_str(columns, *values))
        if on_conflict is not None:
            query += ' ON CONFLICT {}'.format(on_conflict)
        if return_field is not None:
            query += ' RETURNING {}'.format(return_field)
        values_args = list(chain(*values))
        async with self.connection.transaction():
            return await self.connection.fetchval(query, *values_args)

    async def insert_one(self, tname: str, data: dict, return_field='id'):
        '''Insert only one row with the values in `data`'''
        columns = []
        values = []
        for col_name, col_val in data.items():
            columns.append(col_name)
            values.append(col_val)
        return await self.insert(tname, ','.join(columns), *[tuple(values)],
                                 return_field=return_field)

    @staticmethod
    def _insert_values_query_str(columns, *values):
        columns_no = len(columns.split(','))
        first_col_no = 1
        insert_values_list = []
        for _ in values:
            last_col_no = columns_no + first_col_no
            values_query = ', '.join(['${}'.format(c_no) for c_no in
                                      range(first_col_no, last_col_no)])
            insert_values_list.append('({})'.format(values_query))
            first_col_no = last_col_no
        return ', '.join(insert_values_list)

    @staticmethod
    def _generate_query(tname, rdata):
        query_where = []
        arguments = []
        for arg_no, col_name in enumerate(rdata, 1):
            arguments.append(rdata[col_name])
            query_where.append('{} = ${}'.format(col_name, arg_no))
        query = 'SELECT * FROM {} WHERE {}'.format(tname, ' AND '.join(query_where))
        return query, arguments


class IntegrityManager:  # pylint: disable=too-few-public-methods
    '''Check the data that exist already and create the new ones when they do not exist'''

    def __init__(self):
        self.store = defaultdict(set)

    async def create(self, session: DBSession, tname: str, data: dict, field='id'):
        '''Given a key and a dictionary as data
        it will check if the field is already there
        if not it will create the new row in database
        @param session: a DBSession instance
        @param tname: the table name in the database
        @param data: the data to be present in the database
        @param field: the field to use as ID (default: id)'''
        if field not in data:
            raise ValueError('Missing {} field in data'.format(field))
        if data[field] not in self.store[tname]:
            self.store[tname].add(data[field])
            await session.find_or_create(tname, data)


class BulkInsert:
    '''Insert more than one row at a time'''

    def __init__(self, session, tname, columns, bsize=1000):
        self.session = session
        self.tname = tname
        self.columns = columns
        self.bsize = int(bsize)
        self.values = []

    async def insert(self, values: tuple):
        '''Add the new values to the bucket
        If the bucket is full then it will flush the data into the database'''
        self.values.append(values)
        if len(self.values) >= self.bsize:
            await self.flush()

    async def flush(self):
        '''Insert the current data into the database'''
        if self.values:
            await self.session.insert(self.tname, self.columns, *self.values,
                                      on_conflict='DO NOTHING')
            self.values = []

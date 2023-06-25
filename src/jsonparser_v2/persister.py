import logging
import os
import random
import re
from time import sleep
from time import time

from contrib.pyas.src.pyas_v3 import As
from contrib.pyas.src.pyas_v3 import Leaf

from contrib.p4thpydb.db.ts import Ts

from .parser import Parser
from .parser import mapGetterCreator

from contrib.p4thpydb.db.pgsql.differ import QueryFactory as PGSQLQueryFactory
from contrib.p4thpydb.db.pgsql.db import DB as PGSQLDB
from contrib.p4thpydb.db.pgsql.orm import ORM as PGSQLORM
from contrib.p4thpydb.db.pgsql.util import Util as PGSQLUtil

from contrib.p4thpydb.db.sqlite.differ import QueryFactory as SQLiteQueryFactory
from contrib.p4thpydb.db.sqlite.db import DB as SQLiteDB
from contrib.p4thpydb.db.sqlite.orm import ORM as SQLiteORM
from contrib.p4thpydb.db.sqlite.util import Util as SQLiteUtil

logger0 = logging.getLogger('Persister')


class RelationError(Exception):
    pass


def retryHelper(f, r=1, delay=lambda r: 0, preTry=lambda: None, postTry=lambda: None, preExcept=lambda: None,
                postExcept=lambda: None):
    preTry()
    try:
        postTry()
        res = f()
        preExcept()
    except Exception as e:
        postExcept()
        print(e)
        if r > 0:
            print('{} retries left.'.format(r))
            d = delay(r)
            if d > 0:
                print('Waiting for {} seconds....'.format(d + d))
            sleep(d + d)
            if d > 0:
                print('Retrying....')
            return retryHelper(f, r=r - 1, delay=delay,
                               preTry=preTry, postTry=postTry, preExcept=preExcept, postExcept=postExcept)
        raise e
    return res


class PersisterMixin(Leaf):

    @classmethod
    def pgsqlCreate(cls, url=None, schema='public', logger=None, **kwargs):

        db = PGSQLDB(url=url, **kwargs)
        orm = PGSQLORM(db)
        qf = PGSQLQueryFactory()
        util = PGSQLUtil()

        return Persister({
            'db': db,
            'orm': orm,
            'util': util,
            'queryFactory': qf,
            'schema': schema,
            'locktype': 'ACCESS EXCLUSIVE',
            'time': time(),
            'realType': 'DOUBLE PRECISION',
            'logger': logger,
        })

    @classmethod
    def sqliteCreate(cls, filePath, logger=None):

        db = SQLiteDB(fileName=filePath, extensions=[
            'contrib/sqlite3-pcre/pcre.so'
        ])
        orm = SQLiteORM(db)
        qf = SQLiteQueryFactory()
        util = SQLiteUtil()

        return Persister({
            'db': db,
            'orm': orm,
            'util': util,
            'queryFactory': qf,
            'schema': 'main',
            'locktype': None,
            'time': time(),
            'realType': 'REAL',
            'logger': logger,
        })

    @classmethod
    def onNew(cls, self):
        self.logger = self['logger'] if self['logger'] is not None else logger0

    def persist(self, parseree: Parser, t=None, file=None, retries=20):
        _t = t
        _t = self['t'] if _t is None else _t
        _t = time() if _t is None else _t

        configee = parseree.configee

        db = self['db']
        orm = self['orm']
        qf = self['queryFactory']

        def createIndexes(tbl, cols, depth=1):
            def ixName(tbl, cols): return '{}_{}'.format(tbl, '_'.join(cols))

            res = {}

            for col in cols:
                if depth < 1:
                    continue
                key = ixName(tbl, [col])
                qDrop = 'drop index if exists {}'.format(
                    self['util'].quote(ixName(tbl, [col])))

                ixn = self['util'].parseIndexName(ixName(tbl, [col]))
                ixtn = self['util'].parseIndexTableName(tbl)
                qCreate = 'create index if not exists {} on {} ({})'.format(self['util'].quote(ixn),
                                                                            self['util'].quote(
                                                                                ixtn),
                                                                            ','.join(self['util'].quote([col])))
                res[key] = {
                    'table': ixtn,
                    'drop': qDrop,
                    'create': qCreate
                }

                for dol in cols:
                    if depth < 2:
                        continue

                    if dol == col:
                        continue
                    key = ixName(tbl, [col, dol])
                    qDrop = 'drop index if exists {}'.format(
                        self['util'].quote(ixName(tbl, [col, dol])))

                    ixn = self['util'].parseIndexName(ixName(tbl, [col, dol]))
                    ixtn = self['util'].parseIndexTableName(tbl)
                    qCreate = 'create index if not exists {} on {} ({})'.format(self['util'].quote(ixn),
                                                                                self['util'].quote(
                                                                                    ixtn),
                                                                                ','.join(
                                                                                    self['util'].quote([col, dol])))
                    res[key] = {
                        'table': ixtn,
                        'drop': qDrop,
                        'create': qCreate
                    }

            return res

        def dropIndexes(table):
            indexRows = self['db'].queryIndexes(schemaRE=r'^{}$'.format(self['schema']),
                                                tableRE=r'^{}$'.format(table))
            res = 0
            for indexRow in indexRows:
                if indexRow['primary_key']:
                    continue
                q = 'drop index if exists {}.{}'.format(self['util'].quote(indexRow['schema']),
                                                        self['util'].quote(indexRow['index']))
                self['db'].query(q)
                res += 1

            return res

        def createIndexedIndexes(tableSpec):

            tbl = tableSpec['name']
            cols = [
                '{}'.format(c) for c in tableSpec['columnSpecs'].keys() if c != '__id'
            ]
            return createIndexes(tbl, cols)

        def queryColumns(tableName):
            tableRE = '^{schema}[.]{table}$'.format(
                schema=re.escape(self['schema']), table=re.escape(tableName))
            pq = qf.columnsQuery(tableRE=tableRE)
            columns0 = db.query(pq)
            return [row['column'] for row in columns0]

        def addColumn(columns0, tableName, columnName, pKeys=[], typeMap={}):

            typeGetter = mapGetterCreator(
                typeMap, fallback=lambda *args, **kwargs: 'TEXT')
            cns = [columnName] \
                if isinstance(columnName, str) \
                else columnName
            if len(columns0) == 0:
                pk = ''
                if len(pKeys) > 0:
                    pk = ', CONSTRAINT "{}" PRIMARY KEY ({})' \
                        .format('pk_' + tableName,
                                ', '.join(['"' + k + '"' for k in pKeys])
                                )

                q = 'CREATE TABLE "{}"."{}" ({} {})' \
                    .format(self['schema'], tableName,
                            ', '.join(['"' + cn + '" {}'.format(typeGetter(cn)) for cn in cns]), pk)
                qp = q, {}
                db.query(qp)
                columns = queryColumns(tableName)
            else:
                for columnName in cns:
                    if columnName not in columns0:
                        qp = 'ALTER TABLE "{}"."{}" ADD COLUMN "{}" {}' \
                            .format(self['schema'], tableName, columnName, typeGetter(columnName)), {}
                        # print(qp)
                        db.query(qp)
                columns = queryColumns(tableName)
            return columns

        def persistTable(table, file=None):

            typeMap = {}
            staticRowMap = {}
            if file is None or file is not False:
                staticRowMap['file'] = \
                    os.path.splitext(os.path.basename(parseree.configee['fileName']))[0] \
                    if file is None else file
            staticRowMap['__time'] = _t
            typeMap['__time'] = self['realType']

            tableName = table['name']
            columns0 = queryColumns(tableName)

            tableSpec = {
                'name': '{}.{}'.format(self['schema'], tableName),
                'primaryKeys': (configee.getRowIdName(),),
                'columnSpecs': {},
            }

            columns = set(table['columns']).union(set(staticRowMap.keys()))
            columns0 = addColumn(columns0, tableName, configee.getRowIdName(), [
                                 configee.getRowIdName()])
            for column in columns:
                columns0 = addColumn(columns0, tableName, column,
                                     [configee.getRowIdName()], typeMap)
                T = Ts.str
                colType = 'TEXT'
                if column == '__time':
                    T = Ts.float
                    colType = self['realType']

                tableSpec['columnSpecs'][column] = {
                    'definition': "{colType} NOT NULL DEFAULT ''".format(colType=colType),
                    'transform': T,
                }

            rows = [
                {
                    **staticRowMap,
                    **parseree['rowMap'][rowId]
                } for rowId in table['rows']
            ]

            droppedIxCount = dropIndexes(tableName)
            print('Dropped {} indexed for table {}.'.format(
                droppedIxCount, tableName))
            orm.upsert(tableSpec, rows, batchSize=1000)
            return {}

        def indexedTableName(pair):
            return '{}<-{}'.format(pair[0], pair[1])

        def lockTable(table):
            pathRE = '^{}$'.format(self['db'].escapeRE(table))
            tables = self['db'].queryTables(pathRE=pathRE, fetchAll=True)
            if self['locktype'] is not None and len(tables) > 0:
                q = 'LOCK TABLE {} IN {} MODE'.format(
                    self['util'].quote(table), self['locktype'])
                self['db'].query(q, debug=True)

        def persistIndexed(tablePair, indexees):

            def indexHelper():

                indexMap = {}

                def getIndex(key):
                    if not key in indexMap:
                        indexMap[key] = 0

                    indexMap[key] = indexMap[key] + 1
                    return indexMap[key] - 1

                return getIndex

            typeMap = {'__time': self['realType']}
            staticRowMap = {'__time': _t}

            rowIndexName = configee.getRowIndexName()
            tableName = indexedTableName(tablePair)

            columns = [configee.getRowIdName(tn)
                       for tn in tablePair] + [rowIndexName]
            columns0 = set(queryColumns(tableName))
            tableSpec = {
                'name': '{}.{}'.format(self['schema'], tableName),
                'primaryKeys': list(columns),
                'columnSpecs': {},
            }
            columns += list(set(staticRowMap.keys()))

            addColumn(columns0, tableName, columns, columns, typeMap={
                **typeMap,
                **{rowIndexName: 'INTEGER NOT NULL'}
            })
            for col in columns:
                if col == rowIndexName:
                    tableSpec['columnSpecs'][rowIndexName] = {
                        'definition': "INTEGER NULL DEFAULT ''",
                        'transform': Ts.int,
                    }
                else:
                    colType = 'TEXT'
                    T = Ts.str
                    if col == '__time':
                        T = Ts.float
                        colType = self['realType']
                    tableSpec['columnSpecs'][col] = {
                        'definition': f"{colType} NOT NULL DEFAULT ''".format(colType=colType),
                        'transform': T,
                    }

            getIndex = indexHelper()
            rows = [
                {
                    **staticRowMap,
                    **{
                        columns[0]: r[tablePair[0]],
                        columns[1]: r[tablePair[1]],
                        rowIndexName: getIndex(r[tablePair[0]]),
                    }
                } for r in indexees
            ]

            dbIndexSpecs = createIndexedIndexes(tableSpec)
            droppedIxCount = dropIndexes(tableName)
            print('Dropped {} indexed for table {}.'.format(
                droppedIxCount, tableName))
            orm.upsert(tableSpec, rows, batchSize=1000, debug=False)
            return dbIndexSpecs

        dbAllIndexSpecs = {}

        tableNames = list(parseree['tableMap'].keys())
        random.shuffle(tableNames)
        for name in tableNames:

            table = parseree['tableMap'][name]
            if len(table['columns']) < 1:
                continue
            if len(table['rows']) < 1:
                continue

            dbAllIndexSpecs.update(
                retryHelper(lambda: persistTable(table, file), retries,
                            preTry=lambda: self['db'].startTransaction(),
                            postTry=lambda: lockTable(
                                '{}.{}'.format(self['schema'], name)),
                            preExcept=lambda: self['db'].commit(),
                            postExcept=lambda: self['db'].rollback(),
                            delay=lambda r: retries - r)
            )

        indexedNames = list(parseree['indexed'].keys())
        random.shuffle(indexedNames)
        for parentChildPair in indexedNames:
            indexees = parseree['indexed'][parentChildPair]
            if len(indexees) < 1:
                continue

            dbAllIndexSpecs.update(
                retryHelper(lambda: persistIndexed(parentChildPair, indexees), retries,
                            preTry=lambda: self['db'].startTransaction(),
                            postTry=lambda: lockTable(
                                '{}.{}'.format(self['schema'], indexedTableName(parentChildPair))),
                            preExcept=lambda: self['db'].commit(),
                            postExcept=lambda: self['db'].rollback(),
                            delay=lambda r: retries - r)
            )

        for _, dbIndexSpec in dbAllIndexSpecs.items():
            retryHelper(lambda: self['db'].query(dbIndexSpec['create']), retries,
                        preTry=lambda: self['db'].startTransaction(),
                        postTry=lambda: lockTable(dbIndexSpec['table']),
                        preExcept=lambda: self['db'].commit(),
                        postExcept=lambda: self['db'].rollback(),
                        delay=lambda r: retries - r)

        return db

    def report(self):
        res = []
        for name, table in self['tableMap'].items():
            res.append('{table} with {columns} columns and {rows} rows:'
                       .format(table=table['name'], columns=len(table['columns']), rows=len(table['rows'])))
            res.append(' Columns: ' + ', '.join(table['columns']))
            if 'children' in table and len(table['children']) > 0:
                res.append(' Child tables: {}.'.format(
                    ', '.join(table['children'])))
            if 'parent' in table and not table['parent'] is None:
                res.append(' Parent table is {}.'.format(table['parent']))
            res.append('\n')

        return res


Persister = As(PersisterMixin)

import xxhash
import ujson
# import json
import re
import os
import hashlib
import logging
import jsonstreamer as jss
import ramda as R
from uuid import uuid4
from contrib.p4thpy.model import Model
from contrib.p4thpy.collection import Collection

from contrib.p4thpydb.db.sqlite.differ import QueryFactory
from contrib.p4thpydb.db.sqlite.db import DB
from contrib.p4thpydb.db.sqlite.orm import ORM
from contrib.p4thpydb.db.ts import Ts

logger0 = logging.getLogger('Parser')


def groupsCreator(groups: dict):

    def helper(groupId, item):

        if not groupId in groups:
            groups[groupId] = []

        groups[groupId].append(item)

        return groups[groupId]

    return helper


def mapGetterCreator(oldNewMap, tag='', fallback=None):

    def helper(old):
        if old in oldNewMap:
            return oldNewMap[old]
        if tag:
            print(tag, 'missing key', old)
        return old if fallback is None else fallback

    return helper


class RelationError(Exception):
    pass


class IndexedError(Exception):

    def __init__(self, message, *args, logger=logger0, **kwargs):
        super().__init__(message, *args, **kwargs)
        logger.error(message)


class Indexees(Collection):

    @classmethod
    def getIndexId(cls, parentChildPair):
        return (parentChildPair[0], parentChildPair[1])

    def __init__(self, parentChildPair, rows):
        super().__init__(rows)
        self.parentChildPair = tuple(parentChildPair)

    def index(self, parentChildeIdPair):
        parentChildPair = self.parentChildPair
        relation = {
            self.parentChildPair[0]: parentChildeIdPair[0],
            self.parentChildPair[1]: parentChildeIdPair[1],
        }
        self.models.append(relation)
        return parentChildPair


class Relatees(Collection):

    @classmethod
    def getRelationId(cls, parentChildPair):
        return (parentChildPair[0], parentChildPair[1]) \
            if parentChildPair[0] < parentChildPair[1] \
            else (parentChildPair[1], parentChildPair[0])

    def __init__(self, tablePair, rows):
        super().__init__(rows)
        self.tablePair = tablePair

    def relate(self, rowIdPair):
        key = self.getRelationId((self.tablePair[0], self.tablePair[1]))
        relation = {
            self.tablePair[0]: rowIdPair[0],
            self.tablePair[1]: rowIdPair[1],
        }
        self.models.append(relation)
        return key


class Parser(Model):

    def __init__(self, tableName,
                 rowIdNameGetter=None,
                 rowIndexNameGetter=None,
                 rowValueNameGetter=None,
                 hashIdNameGetter=None,
                 hasher=lambda val: xxhash.xxh64(val).hexdigest(),
                 encoding='None', logger=None):

        super().__init__({
            'tableMap': {},
            'rowMap': {},
            'related': {},
            'indexed': {},
            'stateStack': [],
            'db': None,
        })

        self.tableName = tableName
        self.fileName = ''
        self.rowIdNameGetter = rowIdNameGetter
        self.rowIndexNameGetter = rowIndexNameGetter
        self.rowValueNameGetter = rowValueNameGetter
        self.hashIdNameGetter = hashIdNameGetter
        self.hasher = hasher
        self.encoding = encoding
        self.jss = jss.JSONStreamer()  # same for JSONStreamer
        self.logger = logger if not logger is None else logger0
        self.jss.auto_listen(self)

    @property
    def keyStack(self):
        return [s['key'] for s in self['stateStack']]

    @property
    def currentState(self):
        return self['stateStack'][-1]

    @property
    def tableStates(self):
        return [
            self['stateStack'][i]
            for i in range(0, len(self['stateStack']) - 1) \
            # if 'rows' in self['stateStack'][i]
            if not self['stateStack'][i]['key'] in [':array', ':object']
        ]

    @property
    def tableStateKeys(self):
        return [s['key'] for s in self.tableStates]

    @property
    def currentTableState(self):
        return self.tableStates[-1]

    @property
    def keyStates(self):
        return [
            s for s in self['stateStack'] if s['key'][0] != ':'
        ]

    @property
    def keyStatesKeys(self):
        return [
            s['key'] for s in self.keyStates
        ]

    @property
    def currentKeyState(self):
        return self.keyStates[-1]

    @property
    def currentColumn(self):
        return self['stateStack'][-1]['key']

    def getRowIdName(self, parentName=None):
        if not self.rowIdNameGetter is None:
            return self.rowIdNameGetter(parentName)
        if parentName is None:
            res = '__id'
        else:
            res = '{}__id'.format(parentName)
        return res

    def getRowIndexName(self, tableName=None):
        if not self.rowIndexNameGetter is None:
            return self.rowIndexNameGetter(tableName)
        return '__index'

    def getRowValueName(self, tableName=None):
        if not self.rowValueNameGetter is None:
            return self.rowValueNameGetter(tableName)
        return '__value'

    def getRowHashName(self, tableName):
        if not self.rowIdNameGetter is None:
            return self.rowIdNameGetter(tableName)
        return '__hash'

    def getRowHash(self, tableName, row, skipCols=None, skipTables=set([])):
        rowIdName = self.getRowIdName()
        rowHashName = self.getRowHashName(tableName)
        if tableName == 'a' or 'a' in skipTables:
            print('getRowHash(' + tableName + '): ', row, skipTables)
        if rowHashName in row:
            # print(' cached: ' + row[rowHashName])
            return row[rowHashName]
        if tableName in skipTables:
            return '.'
            # return tableName + ' circiular'

        skipCols = (rowIdName, rowHashName) \
            if skipCols is None else skipCols
        rowId = row[rowIdName]
        table = self['tableMap'][tableName]

        sortedKeys = list(row.keys())
        sortedKeys.sort()
        hashBasis = [{key: row[key]
                      for key in sortedKeys if key not in skipCols}]

        indexeds = R.pipe(
            R.to_pairs,
            R.filter(
                lambda pair: pair[0][0] == tableName and pair[0][1] in table['children']),
            R.sort(lambda pairA, pairB: pairA[0][1] < pairB[0][1]),
            R.from_pairs,
        )(self['indexed'])
        for parentChildPair, indexees in indexeds.items():
            childName = parentChildPair[1]

            hashes = [
                self.getRowHash(childName, self['rowMap'][indexee[childName]],
                                skipTables=skipTables.union([tableName]))
                for indexee in indexees
                if indexee[tableName] == rowId
            ]
            hashBasis.append({childName: hashes})

        # print(hashBasis)
        rowHash = ujson.dumps(hashBasis)
        rowHash = rowHash if self.hasher is None else self.hasher(rowHash)
        row[rowHashName] = rowHash
        if tableName == 'a' or 'a' in skipTables:
            print('getRowHash(' + tableName + '): ', rowHash)
        return rowHash

    def getTable(self, tableName):
        if not tableName in self['tableMap']:
            self['tableMap'][tableName] = {
                'name': tableName,
                'rows': set([]),
                'columns': set([]),
                'parent': None,
                'children': set([]),
            }
        return self['tableMap'][tableName]

    def startRow(self, state, key, isIndexd):
        state['rows'] = state['rows'] if 'rows' in state else []
        state['columns'] = state['columns'] if 'columns' in state else set([])
        state['isIndexd'] = state['isIndexd'] if 'isIndexd' in state else isIndexd
        assert state['isIndexd'] == isIndexd

        if len(state['rows']) < 1 \
           or (key is not None and key in state['rows'][-1]):
            state['rows'].append({})

        return len('rows') - 1

    def addValue(self, val, isIndexd):
        assert self.currentKeyState == self.currentState
        key = self.currentKeyState['key']
        state = self.currentTableState
        self.startRow(self.currentTableState, key, isIndexd)
        assert 'rows' in state
        # if len(state['rows']) < 1:
        #    state['rows'].append({})
        row = state['rows'][-1]
        assert key not in row
        row[key] = val
        state['columns'] = state['columns'].union(set([key]))

    def pushState(self, p):
        return self['stateStack'].append(p)

    @classmethod
    def relatee(cls, relation, relater):
        return [t for t in relation if t != relater][0]

    def getRelationId(self, parentChildPair):
        return Relatees.getRelationId(parentChildPair)

    def relate(self, parentChildPair, rowIdPair):
        key = self.getRelationId((parentChildPair[0], parentChildPair[1]))
        self['related'][key] = self['related'][key] \
            if key in self['related'] \
            else []
        relatees = Relatees(parentChildPair, self['related'][key])
        return relatees.relate(rowIdPair)

    def index(self, parentChildPair, rowIdPair):
        key = Indexees.getIndexId(parentChildPair)
        self['indexed'][key] = self['indexed'][key] \
            if key in self['indexed'] \
            else []
        indexees = Indexees(key, self['indexed'][key])
        return indexees.index(rowIdPair)

    def unrelate(self, relation):
        key = self.getRelationId(tuple(relation.keys()))
        return self['related'][key].remove(relation) \
            if relation in self['related'][key] \
            else None

    def unrelateAll(self, key):
        return self['related'].pop(self.getRelationId(key))

    def unindex(self, parentChildPair, indexee):
        if not parentChildPair in self['indexed']:
            raise IndexedError(
                'Cannot unindex, since {} is missing in {}.'
                .format(str(parentChildPair), str(set(self['indexed'].keys()))),
                logger=self.logger,
            )
        return self['indexed'][parentChildPair].remove(indexee)

    def unindexAll(self, key):
        return self['indexed'].pop(key)

    def appendRows(self, state):

        def ensureId(row, table):
            colName = self.getRowIdName()
            id = row[colName] \
                if colName in row else str(uuid4())
            row[colName] = id
            table['columns'].add(colName)
            return id, colName

        if not 'rows' in state:
            return

        # tableName = self.keyStatesKeys[-1]
        tableName = state['key']
        table = self.getTable(tableName)

        hasParent = tableName != self.tableName
        if hasParent:
            parentTableState = self.currentTableState
            parentTableName = parentTableState['key']
            table['parent'] = parentTableName
            parentTable = self.getTable(parentTableName)
            children = parentTable['children']
            children.add(tableName)
            parentTable['children'] = children
        for row in state['rows']:
            if len(row) == 0:
                continue

            id, idCol = ensureId(row, table)

            if hasParent:
                parentIdColumn = self.getRowIdName(parentTableName)
                self.startRow(parentTableState, None, False)
                parentRow = parentTableState['rows'][-1]
                parentId, _ = ensureId(parentRow, parentTable)
                if state['isIndexd']:
                    self.index([parentTableName, tableName],
                               [parentId, id]
                               )
                else:
                    self.relate([parentTableName, tableName],
                                [parentId, id]
                                )

            self['rowMap'][id] = row
            table['rows'].add(id)

        table['columns'] = set(table['columns']).union(set(state['columns']))

    def popState(self):

        # tableName = '_'.join([s['key'] for s in self.tableStates])
        state = self['stateStack'].pop()
        self.appendRows(state)

        return state

    def _on_doc_start(self, *args):
        self.pushState({
            'key': self.tableName,
        })
        self.logger.debug('_doc_start' + str(self.keyStack))

    def _on_doc_end(self, *args):
        assert len(self['stateStack']) == 1
        self.appendRows(self['stateStack'].pop())
        self.logger.debug('_doc_end' + str(self.keyStack))

    def _on_key(self, key, *args):
        self.pushState({'key': key})
        self.logger.debug('_on_key' + str(self.keyStack))

    def _on_value(self, val, *args):
        # print('_on_value', val, *args)
        self.addValue(val, False)
        self.popState()
        self.logger.debug('_on_value ' + str(val) + str(self.keyStack))

    def _on_element(self, val, *args):
        # print('_on_element', val, *args)

        state = self['stateStack'][-1]

        # self.pushState({ 'key': self.getRowIndexName(state['key']) })
        # self.addValue(state['index'], True)
        # self.popState()

        self.pushState({'key': self.getRowValueName(self.keyStatesKeys[-1])})
        self.addValue(val, True)
        self.popState()

        state['index'] = state['index'] + 1
        self.logger.debug('_on_element ' + str(val) + str(self.keyStack))

    def _on_array_start(self, *args):
        # print('_on_array_start', args)
        self.pushState({
            'key': ':array',
            'index': 0,
        })
        self.logger.debug('_on_array_start' + str(self.keyStack))

    def _on_array_end(self, *args):
        # print('_on_array_end', args)
        self.popState()
        self.closeArrayOrObject()
        self.logger.debug('_on_array_end' + str(self.keyStack))

    def _on_object_start(self, *args):
        self.pushState({
            'key': ':object',
        })
        self.logger.debug('_on_object_start' + str(self.keyStack))

    def _on_object_end(self, *args):
        self.popState()
        self.closeArrayOrObject()
        self.logger.debug('_on_object_end' + str(self.keyStack))

    def closeArrayOrObject(self):
        # while len(self['stateStack']) > 1 \
        #      and self.currentColumn not in [':array', ':object']:
        #    self.popState()
        # while len(self['stateStack']) > 1 \
        #      and self.currentColumn not in [':array', ':object']:
        if len(self['stateStack']) > 1 and self.currentState['key'] not in [':array', ':object']:
            self.popState()

    def parse(self, file):

        def read(f):
            self.fileName = f.name
            if self.encoding:
                return f.read().decode(self.encoding)
            else:
                return f.read()

        self.jss.consume(
            file
            if isinstance(file, str)
            else read(file)
        )
        self._on_doc_end()

        self.reduceTables()
        # self.relateIndexed()
        self.reduceRows()

    def _relateIndexed(self):

        def getIndexCreator():

            map = {}

            def helper(parentRowId):
                res = map[parentRowId] \
                    if parentRowId in map else 0
                map[parentRowId] = res + 1
                return res

            return helper

        for parentChildPair, indexeds in self['indexed'].items():
            if len(indexeds) < 1:
                continue
            getIndex = getIndexCreator()
            pName = parentChildPair[0]
            cName = parentChildPair[1]
            childTable = self['tableMap'][cName]
            pTable = self['tableMap'][pName]
            parentIdColumn = self.getRowIdName(pName)
            childTable['columns'].add(parentIdColumn)
            for indexed in indexeds:
                pRowId = indexed[pName]
                pRow = self['rowMap'][pRowId]
                cRowId = indexed[cName]
                self['rowMap'][cRowId].update({
                    parentIdColumn: indexed[pName],
                    # self.getRowIndexName(cName): getIndex(pRowId),
                })

    def reduceTables(self):

        def filterTables():

            oldNewTableMap = {}

            for name, table in self['tableMap'].items():
                nonValueColumns = set([
                    self.getRowIdName(),
                    self.getRowHashName(name),
                ])
                if len(table['columns']
                       .difference(nonValueColumns)) > 0:
                    continue

                parName = table['parent']
                if parName is None:
                    continue
                parTable = self['tableMap'][parName]

                if len(parTable['children']) != 1:
                    continue

                for childName in table['children']:
                    if (name, childName) in self['indexed']:
                        continue

                    oldNewTableMap[childName] = name

            return oldNewTableMap

        def mergeRows(oldNewTableMap):

            tableNewRowsMap = {}
            newRowsGrouper = groupsCreator(tableNewRowsMap)
            tableOrphanRowsMap = {}
            orphanRowsGrouper = groupsCreator(tableOrphanRowsMap)

            oldIdNewIdMap = {}

            for childName, name in oldNewTableMap.items():
                table = self['tableMap'][name]
                parName = table['parent']

                oldChildPair = (name, childName)
                oldChildRelatees = self['related'][self.getRelationId(
                    oldChildPair)]

                rowIdName = self.getRowIdName()
                newParentTablePair = (parName, name)
                for oldChildRelatee in oldChildRelatees:
                    oldChildRowId = oldChildRelatee[childName]
                    oldChildRow = self['rowMap'][oldChildRowId]

                    oldRowId = oldChildRelatee[name]
                    oldRow = self['rowMap'][oldRowId]

                    self.logger.debug('MergeRows: %s:%s \n ~ %s:%s',
                                      name, self['rowMap'][oldRowId], childName, self['rowMap'][oldChildRowId])

                    newRow = {**oldChildRow, ** {rowIdName: str(uuid4())}}
                    newRowId = newRow[rowIdName]

                    newRowsGrouper(name, newRow)
                    orphanRowsGrouper(name, oldRow)
                    orphanRowsGrouper(childName, oldChildRow)

                    oldIdNewIdMap[oldChildRowId] = newRowId
            return tableNewRowsMap, tableOrphanRowsMap, oldIdNewIdMap

        def processRelations(oldNewTableMap, oldIdNewIdMap):
            pairNewRelateesMap = {}
            newRelateesGrouper = groupsCreator(pairNewRelateesMap)
            pairOrphanRelateesMap = {}
            orphaRelateesGrouper = groupsCreator(pairOrphanRelateesMap)
            for childName, name in oldNewTableMap.items():
                table = self['tableMap'][name]
                parName = table['parent']

                oldChildPair = (name, childName)
                oldChildRelatees = self['related'][self.getRelationId(
                    oldChildPair)]

                oldParentPair = (parName, name)
                oldParentRelatees = R.pipe(
                    R.group_by(lambda relatee: relatee[name]),
                )(self['related'][self.getRelationId(oldParentPair)])

                rowIdName = self.getRowIdName()
                newParentTablePair = (parName, name)
                for oldChildRelatee in oldChildRelatees:
                    oldChildRowId = oldChildRelatee[childName]
                    oldChildRow = self['rowMap'][oldChildRowId]

                    oldRowId = oldChildRelatee[name]
                    oldRow = self['rowMap'][oldRowId]

                    oldParentRelatee = oldParentRelatees[oldRowId]
                    assert len(oldParentRelatee) == 1
                    oldParentRelatee = oldParentRelatee[0]
                    oldParentRowId = oldParentRelatee[parName]

                    self.logger.debug('ProcessRelations: %s: %s\n ~ %s:%s\n ~ %s:%s',
                                      parName, self['rowMap'][oldParentRowId],
                                      name, self['rowMap'][oldRowId],
                                      childName, self['rowMap'][oldChildRowId])

                    orphaRelateesGrouper((parName, name), oldParentRelatee)
                    orphaRelateesGrouper((name, childName), oldChildRelatee)

                    newRelateesGrouper(
                        (parName, name),
                        (oldParentRowId, oldIdNewIdMap[oldChildRowId])
                    )
            return pairNewRelateesMap, pairOrphanRelateesMap

        def processIndexes(oldNewTableMap, oldIdNewIdMap):
            newIndexeesMap = {}
            newIndexeesGrouper = groupsCreator(newIndexeesMap)
            orphanIndexeesMap = {}
            orphanIndexeesGrouper = groupsCreator(orphanIndexeesMap)

            oldTables = set(oldNewTableMap.keys())
            indexedKeys = R.pipe(
                R.filter(
                    lambda parentChildPair: parentChildPair[0] in oldTables)
            )(self['indexed'].keys())

            oldIdNewIdGetter = mapGetterCreator(oldIdNewIdMap, 'reduceTables')
            for oldParentChildPair in indexedKeys:
                newParent = oldNewTableMap[oldParentChildPair[0]]
                oldChild = oldParentChildPair[1]
                oldIndexeds = self['indexed'][oldParentChildPair]
                newParentChildPair = (newParent, oldChild)

                self.logger.debug('processIndexes: %s:%s\n ~ %s:%s',
                                  oldParentChildPair[0], oldParentChildPair[1],
                                  newParentChildPair[0], newParentChildPair[1])

                for oldIndexed in oldIndexeds:
                    # print('...', newParentChildPair, oldParentChildPair)
                    newChildRowId = oldIndexed[oldParentChildPair[1]]
                    newChildTable = oldParentChildPair[1]

                    newParentRowId = oldIndexed[oldParentChildPair[0]]
                    newParentRowId = oldIdNewIdGetter(newParentRowId)
                    newParentTable = oldNewTableMap[oldParentChildPair[0]]

                    orphanIndexeesGrouper(oldParentChildPair, oldIndexed)
                    newIndexeesGrouper(
                        (newParentTable, newChildTable),
                        (newParentRowId, newChildRowId)
                    )

                    # self['tableMap'][oldParentChildPair[1]]['parent'] = newParentChildPair[0]
                    # assert 1 == 0

            return newIndexeesMap, orphanIndexeesMap

        oldNewTableMap = filterTables()

        tableNewRowsMap, tableOrphanRowsMap, oldIdNewIdMap \
            = mergeRows(oldNewTableMap)

        pairNewRelateesMap, pairOrphanRelateesMap \
            = processRelations(oldNewTableMap, oldIdNewIdMap)

        newIndexeesMap, orphanIndexeesMap \
            = processIndexes(oldNewTableMap, oldIdNewIdMap)

        self.logger.debug('-+: %s', oldIdNewIdMap)
        self.logger.debug('-> tables: %s', oldNewTableMap)
        self.logger.debug('+ rows: %s', tableNewRowsMap)
        self.logger.debug('- rows: %s', tableOrphanRowsMap)

        self.logger.debug('+ relatees: %s', pairNewRelateesMap)
        self.logger.debug('- relatees: %s', pairOrphanRelateesMap)

        self.logger.debug('+ indexees: %s', newIndexeesMap)
        self.logger.debug('- indexees: %s', orphanIndexeesMap)

        for parentChildTables, indexees in orphanIndexeesMap.items():
            for indexee in indexees:
                self.logger.debug('-indexee: %s', indexee)
                self.unindex(parentChildTables, indexee)

        for tablePair, relatees in pairOrphanRelateesMap.items():
            for relatee in relatees:
                self.logger.debug('-relatee: %s', relatee)
                self.unrelate(relatee)

        for table, rows in tableOrphanRowsMap.items():
            for row in rows:
                rowId = row[self.getRowIdName()]
                self.logger.debug('-row: %s', rowId)
                if not rowId in self['rowMap']:
                    continue
                del self['rowMap'][rowId]
                self['tableMap'][table]['rows'].remove(rowId)

        for table, rows in tableNewRowsMap.items():
            columns = self['tableMap'][table]['columns']
            for row in rows:
                rowId = row[self.getRowIdName()]
                self.logger.debug('+row: %s', rowId)
                assert not rowId in self['rowMap']
                self['rowMap'][rowId] = row
                self['tableMap'][table]['rows'].add(rowId)
                columns = columns.union(set(row.keys()))
            self['tableMap'][table]['columns'] = columns

        for tablePair, rowIdPairs in pairNewRelateesMap.items():
            for rowIdPair in rowIdPairs:
                self.logger.debug('+relatee: %s', rowIdPair)
                self.relate(tablePair, rowIdPair)

        for parentChildTablePair, parentChildIdPairs in newIndexeesMap.items():
            parentName = parentChildTablePair[0]
            cildName = parentChildTablePair[1]
            self['tableMap'][cildName]['parent'] = parentName
            for parentChildIdPair in parentChildIdPairs:
                self.logger.debug('+indexee: %s', parentChildIdPair)
                self.index(parentChildTablePair, parentChildIdPair)

        for oldTable in oldNewTableMap.keys():
            for key in self['tableMap'][oldTable]['rows']:
                del self['rowMap'][key]
            del self['tableMap'][oldTable]

    def reduceRows(self):

        def createIndexHelper(keyIdxMap):

            def getIndex(key):
                if not key in keyIdxMap:
                    keyIdxMap[key] = -1
                keyIdxMap[key] = keyIdxMap[key] + 1
                return keyIdxMap[key]
            return getIndex

        oldNewIdMap = {}

        idCol = self.getRowIdName()
        uniqRows = {}

        for tableName, table in self['tableMap'].items():
            for id in table['rows']:
                row = self['rowMap'][id]

                hashVal = self.getRowHash(tableName, row)

                if id in oldNewIdMap:
                    continue
                oldNewIdMap[id] = hashVal
                if hashVal in uniqRows:
                    continue
                row[self.getRowIdName()] = hashVal
                uniqRows[hashVal] = row

        self['rowMap'] = uniqRows

        for name, table in self['tableMap'].items():
            table['rows'] = set([
                oldNewIdMap[id] for id in table['rows']
            ])

        oldNewIdGetter = mapGetterCreator(oldNewIdMap, 'reduceRows')
        related = {}
        for tablePair, relateds in self['related'].items():
            if len(relateds) < 1:
                continue
            table1, table2 = tablePair
            relateds = [
                {
                    table1: oldNewIdGetter(r[table1]),
                    table2: oldNewIdGetter(r[table2]),
                } for r in relateds
            ]
            relateds = {
                ujson.dumps(r): r for r in relateds
            }

            related[tablePair] = relateds.values()
        self['related'] = related

        newIndexed = {}
        for parentChildNamePair, indexeds in self['indexed'].items():
            if len(indexeds) < 1:
                continue
            parentName, childName = parentChildNamePair
            parentIdCol = self.getRowIdName(parentName)

            indexHelper = createIndexHelper({})

            uniqIndexees = {}
            for i, r in enumerate(indexeds):
                i = indexHelper(r[parentName])
                # print((r[parentName], r[childName]), ' -> ', i)
                r = {
                    parentName: oldNewIdGetter(r[parentName]),
                    childName: oldNewIdGetter(r[childName]),
                }

                uniqIndexees[(i, r[parentName], r[childName])] = r

            newIndexed[parentChildNamePair] = uniqIndexees.values()

        self['indexed'] = newIndexed

    def persist(self, dbPath, file=None):

        db = DB(fileName=dbPath, extensions=[
            'src/sqlite_ext/pcre'
        ])
        qf = QueryFactory()
        orm = ORM(db)

        def queryColumns(tableName):
            tableRE = '^main.{}$'.format(re.escape(tableName))
            pq = qf.columnsQuery(tableRE=tableRE, schema='main')
            columns0 = db.query(pq)
            return [row['column'] for row in columns0]

        def addColumn(columns0, tableName, columnName, pKeys=[], typeMap={}):

            typeGetter = mapGetterCreator(typeMap, fallback='TEXT')
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

                q = 'CREATE TABLE "{}" ({} {})' \
                    .format(tableName, ', '.join(['"' + cn + '" {}'.format(typeGetter(cn)) for cn in cns]), pk)
                qp = q, {}
                db.query(qp)
                columns = queryColumns(tableName)
            else:
                for columnName in cns:
                    if columnName not in columns0:
                        qp = 'ALTER TABLE "{}" ADD COLUMN "{}" {}' \
                            .format(tableName, columnName, typeGetter(columnName)), {}
                        db.query(qp)
                columns = queryColumns(tableName)
            return columns

        def persistTable(table, file=None):
            staticRowMap = {}
            if file is None or file != False:
                staticRowMap['file'] = \
                    os.path.splitext(os.path.basename(self.fileName))[0] \
                    if file is None \
                    else file

            tableName = table['name']
            columns0 = queryColumns(tableName)
            tableSpec = {
                'name': tableName,
                'primaryKeys': (self.getRowIdName(), ),
                'columnSpecs': {},
            }

            columns = set(table['columns']).union(set(staticRowMap.keys()))
            columns0 = addColumn(columns0, tableName, self.getRowIdName())
            for column in columns:
                columns0 = addColumn(columns0, tableName, column,
                                     [self.getRowIdName()])
                tableSpec['columnSpecs'][column] = {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                }
            rows = [
                {
                    **staticRowMap,
                    **self['rowMap'][rowId]
                } for rowId in table['rows']
            ]
            orm.upsert(tableSpec, rows)

        def persistRelation(tablePair, relateds):
            tableName = '{}~{}'.format(tablePair[0], tablePair[1])
            columnPair = tuple('{}{}'.format(t, self.getRowIdName())
                               for t in tablePair)
            columns0 = queryColumns(tableName)
            if len(columns0) > 0:
                if len(columns0) != 2 \
                   or columnPair[0] not in columns0 \
                   or columnPair[1] not in columns0:
                    raise RelationError(
                        'Table {} exists in db but is not compatible (existing columns {}).'.format(tableName, columns0))

            tableSpec = {
                'name': tableName,
                'primaryKeys': columnPair,
                'columnSpecs': {},
            }
            columns0 = addColumn(columns0, tableName, columnPair,
                                 columnPair)
            for col in columnPair:
                tableSpec['columnSpecs'][col] = {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                }
            rows = [
                {
                    columnPair[0]: r[tablePair[0]],
                    columnPair[1]: r[tablePair[1]],
                } for r in relateds
            ]
            orm.upsert(tableSpec, rows)

        def persistIndexed(tablePair, indexees):

            def indexHelper():

                indexMap = {}

                def getIndex(key):
                    if not key in indexMap:
                        indexMap[key] = 0

                    indexMap[key] = indexMap[key] + 1
                    return indexMap[key] - 1

                return getIndex

            rowIndexName = self.getRowIndexName()
            tableName = '{}<-{}'.format(tablePair[0], tablePair[1])
            columns = [self.getRowIdName(tn)
                       for tn in tablePair] + [rowIndexName]
            columns0 = set(queryColumns(tableName))
            if len(columns0) > 0:
                if set(columns) != columns0:
                    raise RelationError(
                        'Table {} exists in db but is not compatible (existing columns {}).'.format(tableName, columns0))

            tableSpec = {
                'name': tableName,
                'primaryKeys': columns,
                'columnSpecs': {},
            }
            columns0 = addColumn(columns0, tableName, columns, columns, typeMap={
                rowIndexName: 'INTEGER NOT NULL'
            })
            for col in columns[:-1]:
                tableSpec['columnSpecs'][col] = {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                }
            tableSpec['columnSpecs'][rowIndexName] = {
                'definition': "INTEGER NULL DEFAULT ''",
                'transform': Ts.int,
            }
            getIndex = indexHelper()
            rows = [
                {
                    columns[0]: r[tablePair[0]],
                    columns[1]: r[tablePair[1]],
                    rowIndexName: getIndex(r[tablePair[0]]),
                } for i, r in enumerate(indexees)
            ]
            orm.upsert(tableSpec, rows)

        db.startTransaction()
        try:
            for name, table in self['tableMap'].items():
                if len(table['columns']) < 1:
                    continue
                if len(table['rows']) < 1:
                    continue
                persistTable(table, file)

            for tablePair, relateds in self['related'].items():
                if len(relateds) < 1:
                    continue
                persistRelation(tablePair, relateds)

            for parentChildPair, indexees in self['indexed'].items():
                if len(indexees) < 1:
                    continue
                persistIndexed(parentChildPair, indexees)

            db.commit()
        except Exception as e:
            db.rollback()
            raise e

        return db

    def report(self):
        res = []
        for name, table in self['tableMap'].items():
            # if len(table['rows']) < 1:
            #    continue
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


if __name__ == '__main__':

    import sys
    import unittest

    # logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN)
    # logging.getLogger().setLevel(logging.ERROR)

    if len(sys.argv) >= 3:
        parser = Parser(sys.argv[2], encoding='utf-8')
        with open(sys.argv[3], 'rb') as f:
            parser.parse(f)
            parser.persist(sys.argv[1])

    class TestParser(unittest.TestCase):

        def testFlat(self):
            data = [
                {
                    'id': 1,
                    'name': 'Kalle',
                    'children': 1,
                },
                {
                    'id': 2,
                    'name': 'Karin',
                    'children': 2
                },
                {
                    'id': 3,
                    'name': 'Kasper',
                    'children': 1
                },
            ]

            parser = Parser('parents', encoding='utf-8')
            parser.parse(ujson.dumps(data))
            self.assertEqual(1, len(parser['tableMap']))
            self.assertTrue('parents' in parser['tableMap'])
            self.assertSetEqual(set(['id', 'name', 'children', '__id']),
                                parser['tableMap']['parents']['columns'])
            self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

            db = parser.persist(':memory:', 'testFlat')
            orm = ORM(db)

            pq = '''
            SELECT _p.*
            FROM "parents" _p
            ORDER BY _p."id"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            # print('parents:\n', rows)
            # First row, Kalle with 1 child
            self.assertEqual('testFlat', rows[0]['file'])
            self.assertEqual('1', rows[0]['id'])
            self.assertEqual('Kalle', rows[0]['name'])
            self.assertEqual('1', rows[0]['children'])
            # Second row, Karin with 2 children
            self.assertEqual('testFlat', rows[1]['file'])
            self.assertEqual('2', rows[1]['id'])
            self.assertEqual('Karin', rows[1]['name'])
            self.assertEqual('2', rows[1]['children'])
            # Third row, Kasper with 1 child
            self.assertEqual('testFlat', rows[2]['file'])
            self.assertEqual('3', rows[2]['id'])
            self.assertEqual('Kasper', rows[2]['name'])
            self.assertEqual('1', rows[2]['children'])

        def testArrayOfValues(self):
            data = [
                {
                    'id': 1,
                    'name': 'Kalle',
                    'children': ['Albert', 'Herbert'],
                },
                {
                    'id': 2,
                    'name': 'Karin',
                    'children': ['Maja']
                },
                {
                    'id': 3,
                    'name': 'Kasper',
                    'children': []
                },
            ]

            # logging.getLogger().setLevel(logging.DEBUG)
            parser = Parser('parents', encoding='utf-8')
            parser.parse(ujson.dumps(data))

            self.assertEqual(2, len(parser['tableMap']))

            self.assertTrue('parents' in parser['tableMap'])
            self.assertSetEqual(set(['id', 'name', '__id']),
                                parser['tableMap']['parents']['columns'])
            self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

            self.assertTrue('children' in parser['tableMap'])
            self.assertSetEqual(set(['__value', '__id']),
                                parser['tableMap']['children']['columns'])
            self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

            self.assertTrue(('parents', 'children') in parser['indexed'])
            self.assertEqual(
                3, len(parser['indexed'][('parents', 'children')]))

            db = parser.persist(':memory:', 'testArrayOfValues')
            orm = ORM(db)

            pq = '''
            SELECT _p.*
            FROM "parents" _p
            ORDER BY _p."id"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            # print('parents:\n', rows)
            # First row, Kalle with 1 child
            self.assertEqual('testArrayOfValues', rows[0]['file'])
            self.assertEqual('1', rows[0]['id'])
            self.assertEqual('Kalle', rows[0]['name'])
            # self.assertEqual(parser.getRowHash({
            #    'id': 1,
            #    'name': 'Kalle',
            # }, '__id'), rows[0]['__id'])
            # Second row, Karin with 2 children
            self.assertEqual('testArrayOfValues', rows[1]['file'])
            self.assertEqual('2', rows[1]['id'])
            self.assertEqual('Karin', rows[1]['name'])
            # self.assertEqual(parser.getRowHash({
            #    'id': 2,
            #    'name': 'Karin',
            # }, '__id'), rows[1]['__id'])
            # Third row, Kasper with 1 child
            self.assertEqual('testArrayOfValues', rows[2]['file'])
            self.assertEqual('3', rows[2]['id'])
            self.assertEqual('Kasper', rows[2]['name'])
            # self.assertEqual(parser.getRowHash({
            #    'id': 3,
            #    'name': 'Kasper',
            # }, '__id'), rows[2]['__id'])

            pq = '''
            SELECT _c.*
            FROM "children" _c
            ORDER BY _c."__value"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            # print('children:\n', rows)
            # First row, Albert
            row = rows[0]
            self.assertSetEqual(
                set(['__id', '__value', 'file']), set(row.keys()))
            self.assertEqual('testArrayOfValues', row['file'])
            self.assertEqual('Albert', row['__value'])
            # Second row, Herbert
            row = rows[1]
            self.assertSetEqual(
                set(['__id', '__value', 'file']), set(row.keys()))
            self.assertEqual('testArrayOfValues', row['file'])
            self.assertEqual('Herbert', row['__value'])
            # Third row, Maja
            row = rows[2]
            self.assertSetEqual(
                set(['__id', '__value', 'file']), set(row.keys()))
            self.assertEqual('testArrayOfValues', row['file'])
            self.assertEqual('Maja', row['__value'])

            pq = '''
            SELECT *
            FROM "parents<-children" _pc
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            row = rows[0]
            self.assertSetEqual(set(row.keys()), set([
                'parents__id', 'children__id', '__index'
            ]))

            pq = '''
            SELECT count(*) "count", sum(__index) sum__index, min(__index) min__index, max(__index) max__index
            FROM "parents<-children" _pc
            ''', {}
            rows = db.query(pq)
            # print(rows)
            self.assertEqual(1, len(rows))
            row = rows[0]
            assert row['count'] == 3
            assert row['min__index'] == 0
            assert row['max__index'] == 1
            assert row['sum__index'] == 1

            pq = '''
            SELECT _p.*, _c.__value as child_name
            FROM "parents" _p
            INNER JOIN "parents<-children" _pc ON _pc.parents__id = _p.__id
            INNER JOIN "children" _c ON _c.__id = _pc.children__id
            ORDER BY _p."id", _c."__value"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            # First row, Kalle with first child Albert
            self.assertEqual('1', rows[0]['id'])
            self.assertEqual('Kalle', rows[0]['name'])
            self.assertEqual('Albert', rows[0]['child_name'])
            # Second row, Kalle with second child Herbert
            self.assertEqual('1', rows[1]['id'])
            self.assertEqual('Kalle', rows[1]['name'])
            self.assertEqual('Herbert', rows[1]['child_name'])
            # Third row, Karin with only child Maja
            self.assertEqual('2', rows[2]['id'])
            self.assertEqual('Karin', rows[2]['name'])
            self.assertEqual('Maja', rows[2]['child_name'])

        def testCoordinateLists(self):
            data = [
                {
                    "id": "1",
                    "coords": [0.0, 80.0, 0.0, 34.1907570770042, 15.2423438211796, 15.4152275177074, 45.7649191770152, 28.2296765855002, 36.3897371374662, 80.0, 0.0, 80.0],
                },
                {
                    "id": "2",
                    "coords": [0.0, 80.0, 0.0, 36.4456276190416, 16.4149992183686, 15.5775091674044, 47.5857772411022, 27.9369810501497, 39.3350438274274, 80.0, 0.0, 80.0],
                }
            ]

            # logging.getLogger().setLevel(logging.DEBUG)
            parser = Parser('objects', encoding='utf-8')
            parser.parse(ujson.dumps(data))

            self.assertEqual(2, len(parser['tableMap']))
            self.assertTrue('objects' in parser['tableMap'])
            self.assertSetEqual(set(['id', '__id']),
                                parser['tableMap']['objects']['columns'])
            self.assertEqual(2, len(parser['tableMap']['objects']['rows']))

            self.assertTrue('coords' in parser['tableMap'])
            self.assertSetEqual(set(['__value', '__id']),
                                parser['tableMap']['coords']['columns'])

            db = parser.persist(':memory:', 'testCoordinateLists')
            orm = ORM(db)

            pq = '''
            SELECT _o.*
            FROM "objects" _o
            ORDER BY _o."id"
            ''', {}
            rows = db.query(pq)
            # print(rows)

            self.assertEqual(2, len(rows))
            row = rows[0]
            self.assertSetEqual(set(row.keys()), set([
                'id', 'file', '__id'
            ]))
            self.assertEqual('1', row['id'])
            self.assertEqual('testCoordinateLists', row['file'])
            row = rows[1]
            self.assertEqual('2', row['id'])
            self.assertEqual('testCoordinateLists', row['file'])

            pq = '''
            SELECT _c.*
            FROM "coords" _c
            ORDER BY _c."__value"
            ''', {}
            rows = db.query(pq)
            # print(rows)

            uniqCoords = R.pipe(
                R.map(lambda o: o['coords']),
                R.unnest,
                R.uniq,
            )(data)
            # print(uniqCoords)
            uniqCoords.sort()

            self.assertEqual(len(uniqCoords), len(rows))
            for i, c in enumerate(uniqCoords):
                self.assertAlmostEqual(float(c), float(rows[i]['__value']), 7)

            pq = '''
            SELECT _oc.*
            FROM "objects<-coords" _oc
            ''', {}
            rows = db.query(pq)
            # for row in rows: print(row)
            self.assertEqual(len(rows),
                             len(data[0]['coords']) + len(data[1]['coords']))

            pq = '''
            SELECT _o.id, _oc.__index, _c.__value
            FROM "objects" _o
            LEFT JOIN "objects<-coords" _oc ON _oc.objects__id = _o.__id
            LEFT JOIN "coords" _c ON _c.__id = _oc.coords__id
            ORDER BY _o.id, _oc.__index
            ''', {}
            rows = db.query(pq)
            # for row in rows: print(row)

            self.assertEqual(
                data[0]['coords'] + data[1]['coords'],
                [float(r['__value']) for r in rows]
            )

        def testAnonymousParents(self):
            data = [
                {
                    'relation': 'Mother',
                    'children': ['Albert', 'Herbert'],
                },
                {
                    'relation': 'Father',
                    'children': ['Albert']
                },
                {
                    'relation': 'Father',
                    'children': ['Herbert',]
                },
            ]
            # logging.getLogger().setLevel(logging.DEBUG)
            parser = Parser('parents', encoding='utf-8')
            parser.parse(ujson.dumps(data))

            self.assertEqual(2, len(parser['tableMap']))

            self.assertTrue('parents' in parser['tableMap'])
            self.assertSetEqual(set(['relation', '__id']),
                                parser['tableMap']['parents']['columns'])
            self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

            self.assertTrue('children' in parser['tableMap'])
            self.assertSetEqual(set(['__value', '__id']),
                                parser['tableMap']['children']['columns'])

            db = parser.persist(':memory:', 'testAnonymousParents')
            orm = ORM(db)

            pq = '''
            SELECT _p.*
            FROM "parents" _p
            ORDER BY _p."relation"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(3, len(rows))
            # print('parents:\n', rows)
            # First row, Kalle with 1 child
            row = rows[0]
            self.assertEqual('testAnonymousParents', row['file'])
            self.assertEqual('Father', row['relation'])
            row = rows[1]
            self.assertEqual('testAnonymousParents', row['file'])
            self.assertEqual('Father', row['relation'])
            row = rows[2]
            self.assertEqual('testAnonymousParents', row['file'])
            self.assertEqual('Mother', row['relation'])

            pq = '''
            SELECT *
            FROM "parents<-children" _pc
            ORDER BY __index
            ''', {}
            rows = db.query(pq)
            # print(rows)
            self.assertEqual(4, len(rows))
            row = rows[0]
            self.assertSetEqual(set(row.keys()), set([
                'parents__id', 'children__id', '__index'
            ]))
            self.assertEqual(0, row['__index'])
            row = rows[1]
            self.assertEqual(0, row['__index'])
            row = rows[2]
            self.assertEqual(0, row['__index'])
            row = rows[3]
            self.assertEqual(1, row['__index'])

            pq = '''
            SELECT _p.*, _pc.__index as child_index, _c.__value as child_name
            FROM "parents" _p
            INNER JOIN "parents<-children" _pc ON _pc.parents__id = _p.__id
            INNER JOIN "children" _c ON _c.__id = _pc.children__id
            ORDER BY _c."__value", _pc.__index, _p.relation
            ''', {}
            rows = db.query(pq)
            # print(rows)
            self.assertEqual(4, len(rows))
            # First row, first child Albert with parent Father
            row = rows[0]
            self.assertEqual('Albert', row['child_name'])
            self.assertEqual('Father', row['relation'])
            self.assertEqual(0, row['child_index'])
            # Second row, first child Albert with parent Mother
            row = rows[1]
            self.assertEqual('Albert', row['child_name'])
            self.assertEqual('Mother', row['relation'])
            self.assertEqual(0, row['child_index'])
            # Third row, second child Herbert with parent Father
            row = rows[2]
            self.assertEqual('Herbert', row['child_name'])
            self.assertEqual('Father', row['relation'])
            self.assertEqual(0, row['child_index'])
            # Fourth row, first child Herbert with parent Mother
            row = rows[3]
            self.assertEqual('Herbert', row['child_name'])
            self.assertEqual('Mother', row['relation'])
            self.assertEqual(1, row['child_index'])

        def test_array_of_objects(self):
            test1Data = [
                {
                    'id': 1,
                    'name': 'Kalle',
                    'children': [
                        {
                            'id': 'a',
                            'name': 'Stina',
                            'school': 'public',
                            'cars': ['Audi', 'Volvo'],
                        }
                    ],
                },
                {
                    'id': 2,
                    'name': 'Karin',
                    'children': [
                        {
                            'id': 'a',
                            'name': 'Stina',
                            'school': 'public',
                            'cars': ['Audi', 'Volvo'],
                        },
                        {
                            'id': 'b',
                            'name': 'Stefan',
                            'school': 'private',
                            'cars': ['Volvo', 'Fiat'],
                        }
                    ],
                },
                {
                    'id': 3,
                    'name': 'Kasper',
                    'children': [
                        {
                            'id': 'b',
                            'name': 'Stefan',
                            'school': 'private',
                            'cars': ['Volvo', 'Fiat'],
                        }
                    ],
                },
            ]
            parser = Parser('parents', encoding='utf-8')
            parser.parse(ujson.dumps(test1Data))
            # print('\n'.join(parser.report()))
            self._testTest1(parser, 'test_array_of_objects')

        def test_object_of_objects(self):
            test1Data = [
                {
                    'id': 1,
                    'name': 'Kalle',
                    'children': {
                        'a': {
                            'id': 'a',
                            'name': 'Stina',
                            'school': 'public',
                            'cars': ['Audi', 'Volvo'],
                        }
                    },
                },
                {
                    'id': 2,
                    'name': 'Karin',
                    'children': {
                        'a': {
                            'id': 'a',
                            'name': 'Stina',
                            'school': 'public',
                            'cars': ['Audi', 'Volvo'],
                        },
                        'b': {
                            'id': 'b',
                            'name': 'Stefan',
                            'school': 'private',
                            'cars': ['Volvo', 'Fiat'],
                        }
                    },
                },
                {
                    'id': 3,
                    'name': 'Kasper',
                    'children': {
                        'b': {
                            'id': 'b',
                            'name': 'Stefan',
                            'school': 'private',
                            'cars': ['Volvo', 'Fiat'],
                        },
                    },
                },
            ]

            parser = Parser('parents', encoding='utf-8')
            parser.parse(ujson.dumps(test1Data))
            # print('\n'.join(parser.report()))
            self._testTest1(parser, 'test_object_of_objects')

        def _testTest1(self, parser, file):
            self.assertTrue('parents' in parser['tableMap'])
            self.assertSetEqual(set(['id', 'name', '__id']),
                                parser['tableMap']['parents']['columns'])
            self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

            self.assertTrue('children' in parser['tableMap'])
            # print(parser['tableMap']['children'])
            self.assertEqual(set(['id', 'name', 'school', '__id'])
                             .difference(parser['tableMap']['children']['columns']), set([]))
            self.assertEqual(2, len(parser['tableMap']['children']['rows']))

            self.assertTrue('cars' in parser['tableMap'])
            self.assertSetEqual(set(['__id', '__value']),
                                parser['tableMap']['cars']['columns'])
            self.assertEqual(3, len(parser['tableMap']['cars']['rows']))

            self.assertIn(('children', 'cars'), parser['indexed'])
            self.assertEqual(4, len(parser['indexed'][('children', 'cars')]))

            db = parser.persist(':memory:', file)
            orm = ORM(db)

            pq = '''
            SELECT id, name, school, file
            FROM "children" _c
            ORDER BY _c."id"
            ''', {}
            rows = db.query(pq)
            self.assertEqual(2, len(rows))
            self.assertEqual({
                'file': file,
                'id': 'a',
                'name': 'Stina',
                'school': 'public',
            }, rows[0])
            self.assertEqual({
                'file': file,
                'id': 'b',
                'name': 'Stefan',
                'school': 'private',
            }, rows[1])

            pq = '''
            SELECT _c.*
            FROM "cars" _c
            ORDER BY _c."__value"
            ''', {}
            rows = db.query(pq)
            # for row in rows: print('car:\n', row)
            self.assertEqual(3, len(rows))
            row = rows[0]
            self.assertEqual('Audi', row['__value'])
            row = rows[1]
            self.assertEqual('Fiat', row['__value'])
            row = rows[2]
            self.assertEqual('Volvo', row['__value'])

            pq = '''
            SELECT *
            FROM "children~parents" _cp
            ''', {}
            rows = db.query(pq)
            self.assertEqual(4, len(rows))
            self.assertEqual(3, len(set([r['parents__id'] for r in rows])))
            self.assertEqual(2, len(set([r['children__id'] for r in rows])))

            pq = '''
            SELECT _p.*, _c.id AS child_id, _c.name as child_name, _cr.__value as car_name, _cc.__index as car_index
            FROM "parents" _p
            LEFT JOIN "children~parents" _cp ON _cp."parents__id" = _p."__id"
            LEFT JOIN "children" _c ON _c."__id" = _cp."children__id"
            LEFT JOIN "children<-cars" _cc ON _cc."children__id" = _c."__id"
            LEFT JOIN "cars" _cr ON _cr."__id" = _cc."cars__id"
            ORDER BY _p."id", _c."id", _cc.__index
            ''', {}
            rows = db.query(pq)
            self.assertEqual(8, len(rows))
            # First row, Kalle with first only child Stina with first car Audi
            row = rows[0]
            self.assertEqual('1', row['id'])
            self.assertEqual('Kalle', row['name'])
            self.assertEqual('a', row['child_id'])
            self.assertEqual('Stina', row['child_name'])
            self.assertEqual('Audi', row['car_name'])
            self.assertEqual(0, row['car_index'])
            # Second row, Kalle with first only child Stina with second car Volvo
            row = rows[1]
            self.assertEqual('1', row['id'])
            self.assertEqual('Kalle', row['name'])
            self.assertEqual('a', row['child_id'])
            self.assertEqual('Stina', row['child_name'])
            self.assertEqual('Volvo', row['car_name'])
            self.assertEqual(1, row['car_index'])
            # Third row, Karin with first child Stina with first car Audi
            row = rows[2]
            self.assertEqual('2', row['id'])
            self.assertEqual('Karin', row['name'])
            self.assertEqual('a', row['child_id'])
            self.assertEqual('Stina', row['child_name'])
            self.assertEqual('Audi', row['car_name'])
            self.assertEqual(0, row['car_index'])

            # Fourth row, Karin with first child Stina with second car Volvo
            row = rows[3]
            self.assertEqual('2', row['id'])
            self.assertEqual('Karin', row['name'])
            self.assertEqual('a', row['child_id'])
            self.assertEqual('Stina', row['child_name'])
            self.assertEqual('Volvo', row['car_name'])
            self.assertEqual(1, row['car_index'])

            # Fifth row, Karin with second child Stefan with first car Volvo
            row = rows[4]
            self.assertEqual('2', row['id'])
            self.assertEqual('Karin', row['name'])
            self.assertEqual('b', row['child_id'])
            self.assertEqual('Stefan', row['child_name'])
            self.assertEqual('Volvo', row['car_name'])
            self.assertEqual(0, row['car_index'])

            # 6th row, Karin with second child Stefan second car Fiat
            row = rows[5]
            self.assertEqual('2', row['id'])
            self.assertEqual('Karin', row['name'])
            self.assertEqual('b', row['child_id'])
            self.assertEqual('Stefan', row['child_name'])
            self.assertEqual('Fiat', row['car_name'])
            self.assertEqual(1, row['car_index'])

            # 7th row, Kasper with only child Stefan with first car Volvo
            row = rows[6]
            self.assertEqual('3', row['id'])
            self.assertEqual('Kasper', row['name'])
            self.assertEqual('b', row['child_id'])
            self.assertEqual('Stefan', row['child_name'])
            self.assertEqual('Volvo', row['car_name'])
            self.assertEqual(0, row['car_index'])

            # 8th row, Kasper with only child Stefan with second car Fiat
            row = rows[7]
            self.assertEqual('3', row['id'])
            self.assertEqual('Kasper', row['name'])
            self.assertEqual('b', row['child_id'])
            self.assertEqual('Stefan', row['child_name'])
            self.assertEqual('Fiat', row['car_name'])
            self.assertEqual(1, row['car_index'])

    unittest.main()

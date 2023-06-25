import ujson
import logging
import jsonstreamer as jss
import ramda as R
from uuid import uuid4

from contrib.pyas.src.pyas_v3 import As
from contrib.pyas.src.pyas_v3 import Leaf

from .config import Config

logger0 = logging.getLogger('Parser')


def groupsCreator(groups: dict):

    def helper(groupId, item):

        if not groupId in groups:
            groups[groupId] = []

        groups[groupId].append(item)

        # print('!x-|>', groupId, item, groups[groupId])
        return groups[groupId]

    return helper


def mapGetterCreator(oldNewMap, tag='', fallback=None):

    def helper(old):
        if old in oldNewMap:
            return oldNewMap[old]
        res = old if fallback is None else fallback(old)
        # if tag:
        #    print(tag, 'missing key', old, '->', res)
        return res

    return helper


class IndexedError(Exception):

    def __init__(self, message, *args, logger=logger0, **kwargs):
        super().__init__(message, *args, **kwargs)
        logger.error(message)


class Indexees:

    @classmethod
    def getIndexId(cls, parentChildPair):
        return (parentChildPair[0], parentChildPair[1])

    def __init__(self, parentChildPair, rows):
        self.models = rows
        self.parentChildPair = tuple(parentChildPair)

    def index(self, parentChildeIdPair):
        parentChildPair = self.parentChildPair
        relation = {
            self.parentChildPair[0]: parentChildeIdPair[0],
            self.parentChildPair[1]: parentChildeIdPair[1],
        }
        self.models.append(relation)
        return parentChildPair


class ParserMixin(Leaf):

    @classmethod
    def onNew(cls, self):
        self.row = {
            **{
                'config': {},
                'logger': logger0,
            },
            **{
                'tableMap': {},
                'rowMap': {},
                'indexed': {},
                'stateStack': [],
            },
            **self.row
        }

        self._configee = Config(self['config'])
        self.jss = jss.JSONStreamer()  # same for JSONStreamer
        self.logger = self['logger']
        self.jss.auto_listen(self)

    @property
    def configee(self):
        return self._configee

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

    def getUniqueRowCount(self, tableName, skipCols=None):
        table = self['tableMap'][tableName]
        uniqs = set([])
        for rowId in table['rows']:
            uniqs.add(self.getRowHash(tableName, self['rowMap'][rowId]))
        return len(uniqs)

    def getRowHash(self, tableName, row, skipCols=None, skipTables=set([])):
        rowIdName = self.configee.getRowIdName()
        rowHashName = self.configee.getRowHashName(tableName)
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

            sum = 0
            for indexee in indexees:
                if indexee[tableName] != rowId:
                    continue
                ch = self.getRowHash(childName, self['rowMap'][indexee[childName]],
                                     skipTables=skipTables.union([tableName]))
                sum += ch
            # print('sum', sum)
            hashBasis.append({childName: str(sum)})

            # hashes = [
            #    self.getRowHash(childName, self['rowMap'][indexee[childName]],
            #                    skipTables=skipTables.union([tableName])) \
            #    for indexee in indexees \
            # ]
            # hashBasis.append({childName: hashes})

        rowHash = ujson.dumps(hashBasis)
        rowHash = rowHash if self.configee['hasher'] is None else self.configee['hasher'](
            rowHash)
        row[rowHashName] = rowHash
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

    def index(self, parentChildPair, rowIdPair):
        key = Indexees.getIndexId(parentChildPair)
        self['indexed'][key] = self['indexed'][key] \
            if key in self['indexed'] \
            else []
        indexees = Indexees(key, self['indexed'][key])
        return indexees.index(rowIdPair)

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
            colName = self.configee.getRowIdName()
            id = row[colName] \
                if colName in row else str(uuid4())
            row[colName] = id
            table['columns'].add(colName)
            return id, colName

        if not 'rows' in state:
            return

        tableName = state['key']
        table = self.getTable(tableName)

        hasParent = tableName != self.configee['rootTableName']
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
                parentIdColumn = self.configee.getRowIdName(parentTableName)
                self.startRow(parentTableState, None, False)
                parentRow = parentTableState['rows'][-1]
                parentId, _ = ensureId(parentRow, parentTable)
                self.index([parentTableName, tableName],
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
            'key': self.configee['rootTableName'],
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

        self.pushState(
            {'key': self.configee.getRowValueName(self.keyStatesKeys[-1])})
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
        if len(self['stateStack']) > 1 and self.currentState['key'] not in [':array', ':object']:
            self.popState()

    def parse(self, file):

        def read(f):
            self.configee['fileName'] = f.name
            if self.configee['encoding']:
                return f.read().decode(self.configee['encoding'])
            else:
                return f.read()

        self.jss.consume(
            file
            if isinstance(file, str)
            else read(file)
        )
        self._on_doc_end()

        self.reduceTables()
        self.reduceRows()

    def reduceTables(self):

        def filterTables():

            oldNewTableMap = {}

            for name, table in self['tableMap'].items():
                nonValueColumns = set([
                    self.configee.getRowIdName(),
                    self.configee.getRowHashName(name),
                ])
                # print(table['columns'], nonValueColumns)
                if len(table['columns']
                       .difference(nonValueColumns)) > 0:
                    continue

                parName = table['parent']
                if parName is None:
                    continue

                for childName in table['children']:

                    childTable = self['tableMap'][childName]
                    if self.getUniqueRowCount(childName) != 1:
                        continue

                    oldNewTableMap[childName] = name

            return oldNewTableMap

        def mergeRows(oldNewTableMap):

            tableNewRowsMap = {}
            newRowsGrouper = groupsCreator(tableNewRowsMap)
            tableOrphanRowsMap = {}
            orphanRowsGrouper = groupsCreator(tableOrphanRowsMap)

            oldIdNewIdsMap = {}
            oldIdNewIdsGrouper = groupsCreator(oldIdNewIdsMap)

            for childName, name in oldNewTableMap.items():
                table = self['tableMap'][name]
                parName = table['parent']

                oldChildPair = (name, childName)
                oldChildIndexees = self['indexed'][Indexees.getIndexId(
                    oldChildPair)]

                rowIdName = self.configee.getRowIdName()
                newParentTablePair = (parName, name)
                for oldChildIndexee in oldChildIndexees:
                    oldChildRowId = oldChildIndexee[childName]
                    oldChildRow = self['rowMap'][oldChildRowId]

                    oldRowId = oldChildIndexee[name]
                    oldRow = self['rowMap'][oldRowId]

                    self.logger.debug('MergeRows: %s:%s \n ~ %s:%s',
                                      name, self['rowMap'][oldRowId], childName, self['rowMap'][oldChildRowId])

                    newRow = {**oldChildRow, ** {rowIdName: str(uuid4())}}
                    newRowId = newRow[rowIdName]

                    newRowsGrouper(name, newRow)
                    orphanRowsGrouper(name, oldRow)
                    orphanRowsGrouper(childName, oldChildRow)

                    oldIdNewIdsGrouper(oldChildRowId, newRowId)
                    oldIdNewIdsGrouper(oldRowId, newRowId)

            return tableNewRowsMap, tableOrphanRowsMap, oldIdNewIdsMap

        def processIndexes(oldNewTableMap, oldIdNewIdsMap):
            oldNewTableGetter = mapGetterCreator(
                oldNewTableMap, 'reduceTables table')
            oldNewIdsGetter = mapGetterCreator(
                oldIdNewIdsMap, 'reduceTables id', fallback=lambda old: [old])

            newIndexeesMap = {}
            newIndexeesGrouper = groupsCreator(newIndexeesMap)
            orphanIndexeesMap = {}
            orphanIndexeesGrouper = groupsCreator(orphanIndexeesMap)

            for oldParentChildPair, oldIndexeds in self['indexed'].items():
                newParentTable = oldNewTableGetter(oldParentChildPair[0])
                newChildTable = oldNewTableGetter(oldParentChildPair[1])
                if newParentTable == newChildTable:
                    continue

                newParentChildPair = (newParentTable, newChildTable)

                self.logger.debug('processIndexes: %s:%s\n ~ %s:%s',
                                  oldParentChildPair[0], oldParentChildPair[1],
                                  newParentChildPair[0], newParentChildPair[1])

                for oldIndexed in oldIndexeds:
                    newChildRowIds = oldIndexed[oldParentChildPair[1]]
                    newChildRowIds = oldNewIdsGetter(newChildRowIds)

                    newParentRowIds = oldIndexed[oldParentChildPair[0]]
                    newParentRowIds = oldNewIdsGetter(newParentRowIds)

                    orphanIndexeesGrouper(oldParentChildPair, oldIndexed)
                    for newChildRowId in newChildRowIds:
                        for newParentRowId in newParentRowIds:
                            newIndexeesGrouper(
                                (newParentTable, newChildTable),
                                (newParentRowId, newChildRowId)
                            )

            return newIndexeesMap, orphanIndexeesMap

        oldNewTableMap = filterTables()

        tableNewRowsMap, tableOrphanRowsMap, oldIdNewIdsMap \
            = mergeRows(oldNewTableMap)

        newIndexeesMap, orphanIndexeesMap \
            = processIndexes(oldNewTableMap, oldIdNewIdsMap)

        self.logger.debug('-+: %s', oldIdNewIdsMap)
        self.logger.debug('-> tables: %s', oldNewTableMap)
        self.logger.debug('+ rows: %s', tableNewRowsMap)
        self.logger.debug('- rows: %s', tableOrphanRowsMap)

        self.logger.debug('+ indexees: %s', newIndexeesMap)
        self.logger.debug('- indexees: %s', orphanIndexeesMap)

        for parentChildTables, indexees in orphanIndexeesMap.items():
            for indexee in indexees:
                self.logger.debug('-indexee: %s', indexee)
                self.unindex(parentChildTables, indexee)

        for table, rows in tableOrphanRowsMap.items():
            for row in rows:
                rowId = row[self.configee.getRowIdName()]
                self.logger.debug('-row: %s', rowId)
                if not rowId in self['rowMap']:
                    continue
                del self['rowMap'][rowId]
                self['tableMap'][table]['rows'].remove(rowId)

        for table, rows in tableNewRowsMap.items():
            columns = self['tableMap'][table]['columns']
            for row in rows:
                rowId = row[self.configee.getRowIdName()]
                self.logger.debug('+row: %s', rowId)
                assert not rowId in self['rowMap']
                self['rowMap'][rowId] = row
                self['tableMap'][table]['rows'].add(rowId)
                columns = columns.union(set(row.keys()))
            self['tableMap'][table]['columns'] = columns

        for parentChildTablePair, parentChildIdPairs in newIndexeesMap.items():
            parentName = parentChildTablePair[0]
            cildName = parentChildTablePair[1]
            self['tableMap'][cildName]['parent'] = parentName
            for parentChildIdPair in parentChildIdPairs:
                self.logger.debug('+indexee: %s', {
                    parentChildTablePair[0]: parentChildIdPair[0],
                    parentChildTablePair[1]: parentChildIdPair[1],
                })
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
                row[self.configee.getRowIdName()] = hashVal
                uniqRows[hashVal] = row

        self['rowMap'] = uniqRows

        for _, table in self['tableMap'].items():
            table['rows'] = set([
                oldNewIdMap[id] for id in table['rows']
            ])

        oldNewIdGetter = mapGetterCreator(oldNewIdMap, 'reduceRows')

        newIndexed = {}
        for parentChildNamePair, indexeds in self['indexed'].items():
            if len(indexeds) < 1:
                continue
            parentName, childName = parentChildNamePair
            parentIdCol = self.configee.getRowIdName(parentName)

            indexHelper = createIndexHelper({})

            uniqIndexees = {}
            for i, r in enumerate(indexeds):
                i = indexHelper(r[parentName])
                r = {
                    parentName: oldNewIdGetter(r[parentName]),
                    childName: oldNewIdGetter(r[childName]),
                }

                uniqIndexees[(i, r[parentName], r[childName])] = r

            newIndexed[parentChildNamePair] = uniqIndexees.values()

        self['indexed'] = newIndexed

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


Parser = As(ParserMixin)

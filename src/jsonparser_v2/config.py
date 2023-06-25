import xxhash

from contrib.pyas.src.pyas_v3 import As
from contrib.pyas.src.pyas_v3 import Leaf


class ConfigMixin(Leaf):

    @classmethod
    def onNew(cls, self):
        self.row = {
            **{
                'rowIdNameGetter': None,
                'rowIndexNameGetter': None,
                'rowValueNameGetter': None,
                'hashIdNameGetter': None,
                'hasher': lambda val: xxhash.xxh64(val).intdigest(),
                'encoding': None,
                'fileName': '',
                'rootTableName': 'root',
            },
            **self.row,
        }

    def getRowIdName(self, parentName=None):
        if not self['rowIdNameGetter'] is None:
            return self['rowIdNameGetter'](parentName)
        if parentName is None:
            res = '__id'
        else:
            res = '{}__id'.format(parentName)
        return res

    def getRowIndexName(self, tableName=None):
        if not self['rowIndexNameGetter'] is None:
            return self['rowIndexNameGetter'](tableName)
        return '__index'

    def getRowValueName(self, tableName=None):
        if not self['rowValueNameGetter'] is None:
            return self['rowValueNameGetter'](tableName)
        return '__value'

    def getRowHashName(self, tableName):
        if not self['hashIdNameGetter'] is None:  # hash!!
            return self['hashIdNameGetter'](tableName)  # hash!!
        return '__hash'


Config = As(ConfigMixin)

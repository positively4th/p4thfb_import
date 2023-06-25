import logging
from uuid import uuid4
import ujson
import ramda as R
import testing.postgresql

from src.jsonparser_v2.parser import Parser
from src.jsonparser_v2.persister import Persister

from contrib.p4thpydb.db.pgsql.db import DB as PGSQLDB

import unittest


class TestParser(unittest.TestCase):

    def setUp(self):
        self.pgSchema = str(uuid4())
        self.postgresql = testing.postgresql.Postgresql()
        db = PGSQLDB(url=self.postgresql.url())
        db.query(('CREATE SCHEMA "{}"'.format(self.pgSchema), {}))

    def tearDown(self):
        self.postgresql.stop()

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

        parseree = Parser({
            'config': {
                'rootTableName': 'parents',
                'encoding': 'utf-8',
            }
        })
        parseree.parse(ujson.dumps(data))
        self.assertEqual(1, len(parseree['tableMap']))
        self.assertTrue('parents' in parseree['tableMap'])
        self.assertSetEqual(set(['id', 'name', 'children', '__id']),
                            parseree['tableMap']['parents']['columns'])
        self.assertEqual(3, len(parseree['tableMap']['parents']['rows']))

        persisterees = [
            Persister.sqliteCreate(':memory:'),
            Persister.pgsqlCreate(
                url=self.postgresql.url(), schema=self.pgSchema)
        ]
        for persisteree in persisterees:
            db = persisteree.persist(parseree, file='testFlat')
            pq = '''
            SELECT _p.*
            FROM "{}"."parents" _p
            ORDER BY _p."id"
            '''.format(persisteree['schema']), {}
            rows = db.query(pq, fetchAll=True)
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
        parser = Parser({
            'config': {
                'rootTableName': 'parents',
                'encoding': 'utf-8'
            }
        })
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
        self.assertEqual(3, len(parser['indexed'][('parents', 'children')]))

        persisteree = Persister.sqliteCreate(':memory:')
        db = persisteree.persist(parser, file='testArrayOfValues')

        pq = '''
        SELECT _p.*
        FROM "parents" _p
        ORDER BY _p."id"
        ''', {}
        rows = db.query(pq, fetchAll=True)
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
        rows = db.query(pq, fetchAll=True)
        self.assertEqual(3, len(rows))
        # print('children:\n', rows)
        # First row, Albert
        row = rows[0]
        self.assertSetEqual(
            set(['__id', '__value', 'file', '__time']), set(row.keys()))
        self.assertEqual('testArrayOfValues', row['file'])
        self.assertEqual('Albert', row['__value'])
        # Second row, Herbert
        row = rows[1]
        self.assertSetEqual(
            set(['__id', '__value', 'file', '__time']), set(row.keys()))
        self.assertEqual('testArrayOfValues', row['file'])
        self.assertEqual('Herbert', row['__value'])
        # Third row, Maja
        row = rows[2]
        self.assertSetEqual(
            set(['__id', '__value', 'file', '__time']), set(row.keys()))
        self.assertEqual('testArrayOfValues', row['file'])
        self.assertEqual('Maja', row['__value'])

        pq = '''
        SELECT *
        FROM "parents<-children" _pc
        ''', {}
        rows = db.query(pq, fetchAll=True)
        self.assertEqual(3, len(rows))
        row = rows[0]
        self.assertSetEqual(set(row.keys()), set([
            'parents__id', 'children__id', '__index', '__time'
        ]))

        pq = '''
        SELECT count(*) "count", sum(__index) sum__index, min(__index) min__index, max(__index) max__index
        FROM "parents<-children" _pc
        ''', {}
        rows = db.query(pq, fetchAll=True)
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
        rows = db.query(pq, fetchAll=True)
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

    def testDoNotReduceOutcome(self):
        data = [
            {
                "id": "1",
                "duel": {
                    "outcome": {
                        "id": 16,
                        "name": "Success In Play"
                    },
                }
            },
            {
                "id": "2",
                "duel": {
                    "outcome": {
                        "id": 12,
                        "name": "From Kick Off"
                    },
                }
            },
        ]

        parser = Parser({
            'config': {
                'rootTableName': 'event',
                'encoding': 'utf-8'
            }})
        parser.parse(ujson.dumps(data))

        persisteree = Persister.sqliteCreate(':memory:')
        file = 'testEmptyLink'
        db = persisteree.persist(parser, file=file)
        pq = '''
        SELECT _e.id event_id, _e.file event_file, 
          _ed.__index duel_index, 
          _d.file duel_file, 
          _do.__index outcome_index, 
          _o.id outcome_id, _o.name outcome_name, _o.file outcome_file
        FROM "event" _e
        LEFT JOIN "event<-duel" _ed ON _ed.event__id = _e.__id
        LEFT JOIN "duel" _d ON _d.__id = _ed.duel__id
        LEFT JOIN "duel<-outcome" _do ON _do.duel__id = _d.__id
        LEFT JOIN "outcome" _o ON _o.__id = _do.outcome__id
        ORDER BY _e.id, _o.id
        ''', {}
        rows = db.query(pq, fetchAll=True)
        # for r in rows: print('event', r)
        self.assertEqual(2, len(rows))
        row = rows[0]
        self.assertEqual('1', row['event_id'])
        self.assertEqual(file, row['event_file'])
        self.assertEqual(0, row['duel_index'])
        self.assertEqual(file, row['duel_file'])
        self.assertEqual(0, row['outcome_index'])
        self.assertEqual('16', row['outcome_id'])
        self.assertEqual('Success In Play', row['outcome_name'])
        self.assertEqual(file, row['outcome_file'])
        row = rows[1]
        self.assertEqual('2', row['event_id'])
        self.assertEqual(file, row['event_file'])
        self.assertEqual(0, row['duel_index'])
        self.assertEqual(file, row['duel_file'])
        self.assertEqual(0, row['outcome_index'])
        self.assertEqual('12', row['outcome_id'])
        self.assertEqual('From Kick Off', row['outcome_name'])
        self.assertEqual(file, row['outcome_file'])

    def testCoordinateLists(self):
        data = [
            {
                "id": "1",
                "coords": [0.0, 80.0, 0.0, 34.1907570770042, 15.2423438211796, 15.4152275177074, 45.7649191770152,
                           28.2296765855002, 36.3897371374662, 80.0, 0.0, 80.0],
            },
            {
                "id": "2",
                "coords": [0.0, 80.0, 0.0, 36.4456276190416, 16.4149992183686, 15.5775091674044, 47.5857772411022,
                           27.9369810501497, 39.3350438274274, 80.0, 0.0, 80.0],
            }
        ]

        # logging.getLogger().setLevel(logging.DEBUG)
        parser = Parser({
            'config': {
                'rootTableName': 'objects',
                'encoding': 'utf-8'
            }})
        parser.parse(ujson.dumps(data))

        self.assertEqual(2, len(parser['tableMap']))
        self.assertTrue('objects' in parser['tableMap'])
        self.assertSetEqual(set(['id', '__id']),
                            parser['tableMap']['objects']['columns'])
        self.assertEqual(2, len(parser['tableMap']['objects']['rows']))

        self.assertTrue('coords' in parser['tableMap'])
        self.assertSetEqual(set(['__value', '__id']),
                            parser['tableMap']['coords']['columns'])

        persisteree = Persister.sqliteCreate(':memory:')
        db = persisteree.persist(parser, file='testCoordinateLists')

        pq = '''
        SELECT _o.*
        FROM "objects" _o
        ORDER BY _o."id"
        ''', {}
        rows = db.query(pq, fetchAll=True)
        # print(rows)

        self.assertEqual(2, len(rows))
        row = rows[0]
        self.assertSetEqual(set(row.keys()), set([
            'id', 'file', '__id', '__time'
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
        rows = db.query(pq, fetchAll=True)
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
        rows = db.query(pq, fetchAll=True)
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
        rows = db.query(pq, fetchAll=True)
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
                'children': ['Herbert', ]
            },
        ]
        # logging.getLogger().setLevel(logging.DEBUG)
        parser = Parser({'config': {
            'rootTableName': 'parents',
            'encoding': 'utf-8'
        }})
        parser.parse(ujson.dumps(data))

        self.assertEqual(2, len(parser['tableMap']))

        self.assertTrue('parents' in parser['tableMap'])
        self.assertSetEqual(set(['relation', '__id']),
                            parser['tableMap']['parents']['columns'])
        self.assertEqual(3, len(parser['tableMap']['parents']['rows']))

        self.assertTrue('children' in parser['tableMap'])
        self.assertSetEqual(set(['__value', '__id']),
                            parser['tableMap']['children']['columns'])

        persisteree = Persister.sqliteCreate(':memory:')
        db = persisteree.persist(parser, file='testAnonymousParents')

        pq = '''
        SELECT _p.*
        FROM "parents" _p
        ORDER BY _p."relation"
        ''', {}
        rows = db.query(pq, fetchAll=True)
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
        rows = db.query(pq, fetchAll=True)
        # print(rows)
        self.assertEqual(4, len(rows))
        row = rows[0]
        self.assertSetEqual(set(row.keys()), set([
            'parents__id', 'children__id', '__index', '__time'
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
        rows = db.query(pq, fetchAll=True)
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
        parser = Parser({
            'config': {
                'rootTableName': 'parents',
                'encoding': 'utf-8'
            }
        })
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

        parser = Parser({
            'config': {
                'rootTableName': 'parents',
                'encoding': 'utf-8'
            }})
        parser.parse(ujson.dumps(test1Data))
        self._testTest1(parser, file='test_object_of_objects')

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

        persisterees = [
            Persister.sqliteCreate(':memory:'),
            Persister.pgsqlCreate(
                url=self.postgresql.url(), schema=self.pgSchema)
        ]
        for persisteree in persisterees:
            db = persisteree.persist(parser, file=file)
            orm = persisteree['orm']

            pq = '''
            SELECT id, name, school, file
            FROM "{}"."children" _c
            ORDER BY _c."id"
            '''.format(persisteree['schema']), {}
            rows = db.query(pq, fetchAll=True)
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
            FROM "{}"."cars" _c
            ORDER BY _c."__value"
            '''.format(persisteree['schema']), {}
            rows = db.query(pq, fetchAll=True)
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
            FROM "{}"."parents<-children" _cp
            '''.format(persisteree['schema']), {}
            rows = db.query(pq, fetchAll=True)
            # for r in rows: print(r)
            self.assertEqual(4, len(rows))
            self.assertEqual(3, len(set([r['parents__id'] for r in rows])))
            self.assertEqual(2, len(set([r['children__id'] for r in rows])))
            self.assertEqual(2, len(set([r['__index'] for r in rows])))
            self.assertEqual(0, min([r['__index'] for r in rows]))
            self.assertEqual(1, max([r['__index'] for r in rows]))

            pq = '''
            SELECT _p.*, _c.id AS child_id, _c.name as child_name, _cr.__value as car_name, _cc.__index as car_index
            FROM "{schema}"."parents" _p
            LEFT JOIN "{schema}"."parents<-children" _cp ON _cp."parents__id" = _p."__id"
            LEFT JOIN "{schema}"."children" _c ON _c."__id" = _cp."children__id"
            LEFT JOIN "{schema}"."children<-cars" _cc ON _cc."children__id" = _c."__id"
            LEFT JOIN "{schema}"."cars" _cr ON _cr."__id" = _cc."cars__id"
            ORDER BY _p."id", _c."id", _cc.__index
            '''.format(schema=persisteree['schema']), {}
            rows = db.query(pq, fetchAll=True)
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


if __name__ == '__main__':

    import sys

    # logging.getLogger().setLevel(logging.DEBUG)
    # logging.getLogger().setLevel(logging.WARN)
    logging.getLogger().setLevel(logging.ERROR)

    if len(sys.argv) >= 3:
        parser = Parser({
            'rootTableName': sys.argv[2],
            'encoding': 'utf-8'
        })
        with open(sys.argv[3], 'rb') as f:
            parser.parse(f)
            persisteree = Persister()
            persisteree.persist(parser, sys.argv[1])

    unittest.main()

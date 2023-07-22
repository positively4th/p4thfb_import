import logging
from multiprocessing import Pool
from .runner import Runner
import zipfile as zf
import re
import json
import ramda as R
import itertools

from contrib.p4thpymisc.src.consoleui import select

from jsonparser_v2.parser import Parser
from jsonparser_v2.persister import Persister


def run1(cmdArgs):
    cmdArgs = cmdArgs
    retVal = cmdArgs[1]
    cmdArgs = cmdArgs[0]
    _self = ImportStatsBombRunner(cmdArgs)
    _self.execute()
    return retVal


class ImportStatsBombRunner(Runner):

    def __init__(self, cmdArgs, *args, **kwargs):
        super().__init__(cmdArgs)

    def formatMatch(self, m, skipProps: bool = False):
        res = '{date} {stage:>10.10} {homeTeam:>15.15} {homeScore} - {awayScore} {awayTeam:<15.15}'.format(
            date=m['match_date'],
            stage=m['competition_stage']['name'],
            homeScore=m['home_score'],
            awayScore=m['away_score'],
            homeTeam=m['home_team']['home_team_name'],
            awayTeam=m['away_team']['away_team_name'],
        )

        if not skipProps:
            res += ' [360: {_360}, id: {id}]'.format(
                id=m['match_id'],
                _360='Yes' if self.has360(
                    self.zipfile, m['match_id']) else 'No',
            )

        return res

    @classmethod
    def has360(cls, zipFile, matchId):
        filePath = 'open-data-master/data/three-sixty/{}.json'.format(matchId)
        try:
            with zf.ZipFile(zipFile, 'r') as files:
                with files.open(filePath, 'r') as _:
                    return True
        except KeyError as e:
            pass
        return False

    def execute(self):

        def formatCompetation(c):
            return '{name:<30.30} [Has 360: {has360} id: {id}]'.format(
                name=c['competition_name'],
                has360=c['match_updated_360'] is not None,
                id=c['competition_id']
            )

        def competitionMenuItems(competitions):
            return {
                formatCompetation(c): c
                for c in competitions
            }

        def matchMenuItems(matches):
            # assert 1 == 0
            ms = sorted(matches, key=lambda m: m['match_date'])
            return {
                self.formatMatch(m): m
                for m in ms
            }

        self.setLogLevel(logging.WARN)

        state = {}
        if self.matchpath is not None:
            matchIds = self.matchpath.split('/')
            competitionId = matchIds.pop(0)
            seasonId = matchIds.pop(0)

            state['selectedCompetition'] = {
                'competition_id': competitionId, 'season_id': seasonId}
            state['matches'] = self.extractMatches(
                self.zipfile, state['selectedCompetition']['competition_id'])

            state['selectedMatches'] = R.filter(lambda match: str(
                match['match_id']) in matchIds)(state['matches'])

        quit = False
        while not quit:
            if 'competitions' not in state:
                state['competitions'] = self.extractCompetitions(self.zipfile)
                continue
            if 'selectedCompetition' not in state:
                state['selectedCompetition'] = select(
                    'Select competition', competitionMenuItems(state['competitions']), default='11')
                continue
            elif 'matches' not in state:
                state['matches'] = self.extractMatches(
                    self.zipfile, state['selectedCompetition']['competition_id'])
                continue
            elif 'selectedMatches' not in state:
                state['selectedMatches'] = select(
                    'Select matches:', matchMenuItems(state['matches']), countLimit=None)
                continue
            elif 'parserees' not in state:
                res = self.parseMatches(
                    state['selectedCompetition']['competition_id'], state['selectedMatches'])
                if res is None:
                    return
                state['parserees'] = res
                continue
            elif 'confirmed' not in state:
                if 'confirmed' in state and state['confirmed'] is None:
                    return
                state['confirmed'] = self.reportTables(
                    state['parserees'], quiet=self.quiet)
                continue
            self.persistTables(state['confirmed'])
            break
        print('')

    def persistTables(self, parserees):
        # overwrite = InputTools.createOverwriter()
        persiterees = {}
        if self.sqlitefile:
            persiterees[self.sqlitefile] = Persister.sqliteCreate(
                self.sqlitefile)
        if self.pgurl:
            persiterees[self.pgurl] = Persister.pgsqlCreate(self.pgurl)

        for index, parseree in enumerate(parserees):
            for type, persister in persiterees.items():
                statusId = self.showStatus('Persisting data from file {}/{}: {} -> {}.'
                                           .format(index + 1, len(parserees), parseree.configee['fileName'], type)
                                           )
                try:
                    persister.persist(parseree)
                finally:
                    self.hideStatus(statusId)

    def reportTables(self, parserees, quiet=False):
        res = []
        for parseree in parserees:
            table = parseree['table']
            parser = parseree['parser']
            if not quiet:
                print('Summary for {}:\n'.format(table))
                print('\n'.join(parser.report()))
            res.append(parser)
        persist = quiet or select('Persist data?', {
            'Yes': True,
            'No': False,
        }, default='Yes')
        if not persist:
            return []
        return res

    def parseMatches(self, competitionId, selectedMatches):

        res = []
        if len(selectedMatches) < 2:
            # overwrite = InputTools.createOverwriter()
            for index, match in enumerate(selectedMatches):
                prefix = 'Parsing Match {}/{}/{}. File {}/{}: '\
                    .format(competitionId, match['season']['season_id'], match['match_id'], index+1,
                            len(selectedMatches))
                res = res + self.parseMatch(self.zipfile, competitionId,  match['season']['season_id'],  match['match_id'],
                                            statusPrefix=prefix  # ,
                                            # overwrite=overwrite
                                            )
            return res

        def matchPath(cid, m):
            return '{}/{}/{}'.format(competitionId, m['season']['season_id'],  m['match_id'])

        args = [(
            {
                **R.omit('statusBar')(self.cmdArgs),
                **{
                    'quiet': True,
                    'matchpath': matchPath(competitionId, m)
                }
            },
            self.showStatus('Processing match {} ({}) in child process.'.format(
                self.formatMatch(m, skipProps=True), matchPath(competitionId, m)))
        )
            for m in selectedMatches
        ]

        with Pool(processes=5) as p:
            statusIds = p.map(run1, args)
            for statusId in statusIds:
                self.hideStatus(statusId)
        return None

    def parseMatch(self, zipPath, competitionId, seasonId, matchId, statusPrefix=''
                   # , overwrite=None
                   ):

        def parseFile(table, f):
            parser = Parser({
                'config': {
                    'rootTableName': table,
                    'encoding': 'utf-8'
                }
            })
            parser.parse(f)
            return parser

        filePaths = {
            'matches': 'open-data-master/data/matches/{}/{}.json'.format(competitionId, seasonId),
            'lineups': 'open-data-master/data/lineups/{}.json'.format(matchId),
            'events': 'open-data-master/data/events/{}.json'.format(matchId),
        }

        if self.has360(zipPath, matchId):
            filePaths['threesixty'] = 'open-data-master/data/three-sixty/{}.json'.format(
                matchId)

        res = []
        with zf.ZipFile(zipPath, 'r') as files:
            for table, filePath in filePaths.items():

                with files.open(filePath, 'r') as f:
                    statusId = self.showStatus('{statusPrefix}Parsing {filePath}'.format(
                        statusPrefix=statusPrefix, filePath=filePath))
                    try:
                        res.append({
                            'table': table,
                            'parser': parseFile(table, f),
                        })
                    finally:
                        self.hideStatus(statusId)
        return res

    def extractCompetitions(self, zipPath):
        with zf.ZipFile(zipPath, 'r') as files:
            with files.open('open-data-master/data/competitions.json') as cs:
                cs = cs.read()
        return json.loads(cs)

    def extractMatches(self, zipPath, competitionId):
        pattern = re.compile(
            'open-data-master/data/matches/{}/.*[.]json'.format(competitionId))
        with zf.ZipFile(zipPath, 'r') as files:
            ns = files.namelist()

            ms = list(itertools.chain(*[
                json.loads(files.read(n)) for n in ns if pattern.match(n)
            ]))
        return ms

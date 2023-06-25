import click
import logging
import os
import multiprocessing as mp
import bottombar as bb

from src.statusbar import getStatusBar


@click.group()
@click.pass_context
def main(ctx):
    pass


@main.command()
@click.option('--zipfile', required=True, help='Path to StatsBomb open data zip file.')
@click.option('--sqlitefile', required=False, help='Path to sqlite db file.')
@click.option('--pgurl', required=False, help='Postgresql db url.')
@click.option('--matchpath', required=False, default=None, help='StatsBomb match id (competition/match).')
@click.option('--quiet', is_flag=True, default=False, required=False, help='No prompting, assume Yes.')
@click.pass_context
def importstatsbomb(ctx, zipfile: str, sqlitefile: str, pgurl: str, matchpath: str, quiet: bool):
    ctx.obj.update({
        'sciptname': os.path.abspath(__file__),
        'zipfile': zipfile,
        'sqlitefile': sqlitefile,
        'pgurl': pgurl,
        'matchpath': matchpath,
        'quiet': quiet,
    })
    from runners.importstatsbomb import ImportStatsBombRunner
    runner = ImportStatsBombRunner(ctx.obj)
    runner.execute()


def start():
    main(obj={
        'statusBar': getStatusBar()
    })


if __name__ == '__main__':
    mp.set_start_method('forkserver')
    logging.getLogger().setLevel(logging.WARN)
    start()

import click
import logging




@click.group()
@click.pass_context
def main(ctx):
    pass

    
@main.command()
@click.option('--zipfile', required=True, help='Path to StatsBomb open data zip file.')
@click.option('--dbfile', required=True, help='Path to sqlite db file.')
@click.pass_context
def importstatsbomb(ctx, zipfile: str, dbfile: str):
        ctx.obj.update({
            'zipfile': zipfile,
            'dbfile': dbfile,
        })
        from runners.importstatsbomb import ImportStatsBombRunner
        print(ctx.obj)
        runner = ImportStatsBombRunner(ctx.obj)
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            logger.setLevel(logging.WARN)
        runner.execute()

        
def start():
    main(obj={})
    
if __name__ == '__main__':


    start()
    

import click
import scripts


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    click.echo("Initializing database from ./data")
    scripts.init_db.run()
    click.echo("Initializing database complete")


@cli.command()
def collect_transfers():
    click.echo("Collecting ERC20 transfers")
    scripts.collect_transfers.run()
    click.echo("Collecting ERC20 transfers complete")


if __name__ == "__main__":
    cli()

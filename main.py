import click
import scripts


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    """Initializes the PostgreSQL database with the JSON files in ./data."""

    click.echo("Initializing database from ./data")

    scripts.init_db.run()

    click.echo("Initializing database complete")


@cli.command()
def collect_transfers():
    """Collects ERC20 transfer data from the Etherscan API."""

    click.echo("Collecting ERC20 transfers from Etherscan")

    scripts.collect_transfers.run()

    click.echo("Collecting ERC20 transfers complete")


if __name__ == "__main__":
    cli()

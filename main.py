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
def clear_transfers():
    """drops the contracts and transfers in the PostgreSQL database."""

    click.echo("Dropping contracts and transfers from database")

    from data.base import Session
    from data.models import Contract, Transfer

    with Session() as session:
        session.query(Contract).delete()
        session.query(Transfer).delete()
        session.commit()

    click.echo("Dropping tables complete")


@cli.command()
@click.argument("path", type=click.Path(exists=True))
def collect_transfers(path):
    """Temporarily dumps the ERC20 transfer data in PATH,
    then registers the data in the PostgreSQL database.
    """

    click.echo("Collecting ERC20 transfers")

    scripts.collect_transfers.run(path=path)

    click.echo("Collecting ERC20 transfers complete")


if __name__ == "__main__":
    cli()

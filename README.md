# Master Thesis

Source code for the Master Thesis,  
*Capital Adequacy Ratios of Decentralized Finance Protocols*

## Prerequisites

### Install Docker Compose

This codebase uses Docker Compose, see [the installation guide](https://docs.docker.com/compose/install/) to install Docker and Docker Compose.  
The structure of the Compose file assumes that the Docker supports version 3.x Compose files, see [the Compose and Docker compatibility matrix](https://docs.docker.com/compose/compose-file/compose-file-v3/) if you have problems running the compose commands.

### Environment Variables

You need to create a `.env` file that contains the following variables:
```.env
# Ethereum
ETHERSCAN_TOKEN=[Your Etherscan API token]

# PostgreSQL
POSTGRES_USER=[Username for PostgreSQL]
POSTGRES_PASSWORD=[Password for PostgreSQL]
POSTGRES_HOST=[Hostname for PostgreSQL, e.g. "localhost" or "host.docker.internal"]
POSTGRES_DB=[Database name in PostgreSQL]
POSTGRES_PORT=[Port number for PostgreSQL]
```
where you need to replace the square brackets "[ ]" with the actual variables, e.g., `POSTGRES_PORT=5432`.  

The Etherscan API key is not mandatory for running this code base, but Etherscan applies stricter rate limits when not including the API key in the queries.
So either you could [create the key for free](https://docs.etherscan.io/getting-started/viewing-api-usage-statistics) or increase [the backoff parameters in the tracker](services/tracker/src/transfers.py#L25-L26)

You can copy paste the `.env.example` file and use this as a template.


## Usage

To run all services:
```bash
docker-compose up --build
```

If you are hosting the PostgreSQL in another host, you may want to run only the `tracker` and `server` services:
```bash
docker-compose up --build tracker server
```

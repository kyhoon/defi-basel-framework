import json
import os
import pickle
from decimal import Decimal

import networkx as nx
import pandas as pd
from tqdm import tqdm

DATA_DIR = "../data"

# read addresses
with open(os.path.join(DATA_DIR, "addresses.json"), "r") as f:
    addresses = json.load(f)
targets = [addresses["treasury"]] + addresses["addresses"]

security_tokens = os.listdir(os.path.join(DATA_DIR, "tokens", "security"))
security_tokens = [token.split(".")[0] for token in security_tokens]

utility_tokens = os.listdir(os.path.join(DATA_DIR, "tokens", "utility"))
utility_tokens = [token.split(".")[0] for token in utility_tokens]

G = nx.DiGraph()
G.add_nodes_from(targets)
G.add_node("external")

for address in tqdm(targets):
    df = pd.read_csv(os.path.join(DATA_DIR, "transfers", address + ".csv"), index_col=0)

    try:
        inflow = df[df["to"] == address]
    except:
        continue
    for idx, row in inflow.iterrows():
        token = row.address.lower()
        if token in security_tokens:
            path = os.path.join(DATA_DIR, "tokens", "security", token + ".csv")
        elif token in utility_tokens:
            path = os.path.join(DATA_DIR, "tokens", "utility", token + ".csv")
        else:
            continue

        try:
            df_price = pd.read_csv(path, index_col=0)
        except:
            continue

        df_price = df_price[df_price.block_number == row.blockNumber]
        if len(df_price) == 0:
            continue

        price = Decimal(df_price.price.values[0].item())
        decimals = Decimal(df_price.decimals.values[0].item())
        value = price * Decimal(row.value) / (10**decimals)

        from_address = row["from"]
        if from_address not in G.nodes:
            from_address = "external"
        if not G.has_edge(from_address, address):
            G.add_edge(
                from_address, address, security=Decimal(0.0), utility=Decimal(0.0)
            )

        if token in security_tokens:
            G[from_address][address]["security"] += value
        else:
            G[from_address][address]["utility"] += value

    try:
        outflow = df[df["from"] == address]
    except:
        continue

    for idx, row in inflow.iterrows():
        token = row.address.lower()
        if token in security_tokens:
            path = os.path.join(DATA_DIR, "tokens", "security", token + ".csv")
        elif token in utility_tokens:
            path = os.path.join(DATA_DIR, "tokens", "utility", token + ".csv")
        else:
            continue

        try:
            df_price = pd.read_csv(path, index_col=0)
        except:
            continue

        df_price = df_price[df_price.block_number == row.blockNumber]
        if len(df_price) == 0:
            continue

        price = Decimal(df_price.price.values[0].item())
        decimals = Decimal(df_price.decimals.values[0].item())
        value = price * Decimal(row.value) / (10**decimals)

        to_address = row["to"]
        if to_address not in G.nodes:
            to_address = "external"
        if not G.has_edge(address, to_address):
            G.add_edge(address, to_address, security=Decimal(0.0), utility=Decimal(0.0))

        if token in security_tokens:
            G[address][to_address]["security"] += value
        else:
            G[address][to_address]["utility"] += value

# save graph object to file
with open(os.path.join(DATA_DIR, "graph.pkl"), "wb") as f:
    pickle.dump(G, f)

# %%
import os
import pickle

DATA_DIR = "../data"

with open(os.path.join(DATA_DIR, "graph.pkl"), "rb") as f:
    G = pickle.load(f)

# %%
import matplotlib.pyplot as plt
import networkx as nx

if "external" in G.nodes:
    G.remove_node("external")

plt.figure(figsize=(10, 10))
pos = nx.kamada_kawai_layout(G)
_ = nx.draw(G, pos=pos, alpha=0.3)
_ = nx.draw_networkx_labels(
    G, pos, labels={"0xFEB4acf3df3cDEA7399794D0869ef76A6EfAff52": "treasury"}
)

# %% degrees
from pprint import pprint

in_degrees = list(G.in_degree(G.nodes))
in_degrees = sorted(in_degrees, key=lambda t: -t[1])
print("in-degree")
pprint(in_degrees[:10])

out_degrees = list(G.out_degree(G.nodes))
out_degrees = sorted(out_degrees, key=lambda t: -t[1])
print("out-degree")
pprint(out_degrees[:10])

# %% amount of netflow

netflows = []
for node in G.nodes:
    in_edges = G.in_edges(node, data=True)
    inflows = sum([e[2]["security"] + e[2]["utility"] for e in in_edges])

    out_edges = G.out_edges(node, data=True)
    outflows = sum([e[2]["security"] + e[2]["utility"] for e in out_edges])

    netflow = inflows - outflows
    netflows.append((node, netflow))
netflows = sorted(netflows, key=lambda t: -t[1])
print("net money flow")
pprint(netflows[:10])

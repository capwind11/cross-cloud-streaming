from random import choice
from copy import deepcopy
import networkx as nx
import matplotlib.pyplot as plt


def init_graph():
    G = nx.Graph()
    edges = []
    with open('data/Example1.txt', 'r') as graphInput:
        for line in graphInput:
            e1, e2, weight = line.split()
            G.add_edge(e1, e2, weight=weight)
    nx.draw(G,with_labels=True)
    plt.show()
    return G


def init_graph(edges):
    G = nx.Graph()
    for edge in edges:
        G.add_edge(edge[0], edge[1], weight = edge[2])
    nx.draw(G,with_labels=True)
    plt.show()
    return G


def karger(G):
    while(len(G.nodes())>2):
        u = choice(list(G.nodes()))
        v = list(G[u])[0]
        neighbours_v = dict(G[v])
        G.remove_node(v)
        del neighbours_v[u]
        for i in neighbours_v:
            if(G.has_edge(u,i)):
                G[u][i]['weight'] += neighbours_v[i]['weight']
            else:
                G.add_edge(u,i,weight=neighbours_v[i]['weight'])
    return G[list(G.nodes())[0]][list(G.nodes())[1]]['weight'],G


def merge(G,s,t):
    neighbours = dict(G[t])
    G.remove_node(t)
    for i in neighbours:
        if(s==i):
            pass
        elif(G.has_edge(s,i)):
            G[s][i]['weight'] += neighbours[i]['weight']
        else:
            G.add_edge(s,i,weight=neighbours[i]['weight'])
    return G

def min_cut(G,s,clo):
    if(len(G)>2):
        clo = max(G[s].items(),key=lambda x:x[1]['weight'])[0]
        merge(G,s,clo)
        return min_cut(G,s,clo)
    else:
        return list(dict(G[s]).keys())[0],clo,list(dict(G[s]).values())[0]['weight']

def stoer_wagner(G,global_cut,u_v,s):
    #print("number of points:",len(G))
    if(len(G)>2):
        clo = 0
        u,v,w = min_cut(deepcopy(G),s,clo)
        merge(G,u,v)
        if(w<global_cut):
            global_cut = w
            u_v = (u,v)
        return stoer_wagner(G,global_cut,u_v,s)
    else:
        last_cut = list(dict(G[s]).values())[0]['weight']
        if(last_cut<global_cut):
            global_cut = last_cut
            u_v = (s,list(G[s])[0])
        return global_cut,u_v

if __name__ == '__main__':
    G = init_graph()
    cut = [karger(deepcopy(G)) for i in range(100)]
    cut.sort(key=lambda x:x[0])
    nx.draw(cut[0][1], with_labels=True)
    plt.show()
    print("global min cut:",cut[0][0],"\nnodes:",cut[0][1].nodes())
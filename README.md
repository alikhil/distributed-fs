# Distributed File System

DFS consist of 2 types of nodes:

* Peer node - where data stored
* Record ID Resolver(RIR) or master node - Knows where and which record is stored for each file

## How it works

Distributes all files among all the peers.

Each file has it row length.

Peer id is calculated by `(row id) % (number of peers)`.

**DFS is not fault tolerant!** If one of the peer nodes stops you will not be able to read/write records from it.

## How to start

1) Run master node and keep it in waiting mode untill N(passed with arguments) peers are connected
2) Run needed number of peer nodes, connect them to master
3) Resume master node by closing possibility to connect for other peers

**WARN!** if you want to start peer with files from old then **start peers in the same order as in first run** 

## How to stop

Stop cluster by stopping master node. It will safely stop all the peers.

## Example

You need 4 terminals

```bash
# Run in terminal #1
./master -peers=3

# Run in terminal #2
./peer -dbdir=peer1 -port=5021
# Run in terminal #3
./peer -dbdir=peer2 -port=5022
# Run in terminal #4
./peer -dbdir=peer3 -port=5023

```

Then connect to master using endpoint found in it's logs
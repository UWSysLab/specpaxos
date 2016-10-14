# Speculative Paxos

This is an implementation of the Speculative Paxos protocol, as
described in the paper
["Designing Distributed Systems Using Approximate Synchrony in Datacenter Networks"](https://drkp.net/papers/specpaxos-nsdi15.pdf)
from NSDI 2015.

Speculative Paxos is a state machine replication protocol based on the
idea of co-designing distributed systems with the datacenter
network. It offers excellent performance in environments where
replicas receive most requests in the same order.  The NSDI paper
describes how to implement a "Mostly-Ordered Multicast" (MOM)
primitive that provides this property with high probability.

In these environments, Speculative Paxos is able to complete
operations without coordination. This allows it to achieve the
theoretical minimum latency (one round trip from client to replicas)
and high throughput, as replicas do not need to exchange messages in
the normal case.

## Contents

This repository contains implementations of 3 replication protocols:

1. Speculative Paxos, including normal operation, synchronization, and
reconciliation protocols.

2. Viewstamped Replication (VR), aka Multi-Paxos, as described in the
   paper
   ["Viewstamped Replication Revisited"](http://pmg.csail.mit.edu/papers/vr-revisited.pdf),
   including an optional batching optimization

3. Fast Paxos, although only the normal case is implemented.

4. A simple unreplicated RPC protocol for comparison

...as well as three applications:

1. a "null RPC" benchmark that executes operations as quickly as
   possible

2. a simple timestamp server, as might be used in a distributed
   storage system

3. a transactional key-value store ("nistore") that supports
   multiversioning and concurrency control using either strict
   two-phase locking or optimistic concurrency control. (This is
   derived from the codebase used in the
   [TAPIR](http://syslab.cs.washington.edu/research/tapir/)
   project.

## Building and Running

Speculative Paxos and the applications can be built using `make`. It
has been tested on Ubuntu 14.04, 16.04 and Debian 8. Regression tests
can be run with `make check`

Dependencies include (Debian/Ubuntu packages):
  protobuf-compiler pkg-config libunwind-dev libssl-dev libprotobuf-dev libevent-dev libgtest-dev

You will need to create a configuration file with the following
syntax:

```
f <number of failures tolerated>
replica <hostname>:<port>
replica <hostname>:<port>
...
```

You can then start a replica with `./bench/replica -c <path to config file> -i <replica number> -m <mode>`, where `mode` is either:
  - `spec` for Speculative Paxos
  - `vr` for Viewstamped Replication
  - `fastpaxos` for Fast Paxos
  - `unreplicated` for no replication (uses only the first replica)

The `vr` mode also accepts a `-b` option to specify the maximum batch
size.

To run a single client, use `./bench/replica -c <path to config file>
-m <mode>`.

For performance measurements, you will likely want to add `-DNASSERT`
and `-O2` to the `CFLAGS` in the Makefile, and run `make PARANOID=0`,
which disables complexity-changing assertions.

## Contact

Speculative Paxos is a product of the
[UW Systems Lab](http://syslab.cs.washington.edu/). Please email Dan
Ports at drkp@cs.washington.edu with any questions.

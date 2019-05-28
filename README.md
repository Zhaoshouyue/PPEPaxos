# PPEPaxos
the consensus algorithm Partition  Processing Egalitarian Paxos
# Three folders：
### 1.mypaxos-latency， which is the latency code
### 2.mypaxos-throughput-1KB， which is the throughput code when the client command is 1KB
### 3.mypaxos-throughput-16B， which is the throughput code when the client command is 16B
# You may need to run these command:
### go install ppmaster
### go install server
### go install client
# You can run through:
### bin/ppmaster &
### bin/server & (3 or 5 times, depend on your server numbers)
### bin/client

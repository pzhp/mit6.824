# mit6.824

# summary

#Lab1#
map reduce task number and id, map => reduce => merge
Master crash, recover from check-point, or give up on job
Worker crash
Straggler task, re-dispatch it
Duplicated task: GFS has atomic rename that prevents output from being visible until complete.
The server's Get() and Put() handlers, Must lock, since RPC library creates per-request goroutines

## "Best effort" vs "exactly once" vs "at-most-once"##

some at-most-once complexities
  this will come up in lab 3
  how to ensure XID is unique?
    big random number?
    combine unique client ID (ip address?) with sequence #?
  server must eventually discard info about old RPCs
    when is discard safe?
    idea:
      each client has a unique ID (perhaps a big random number)
      per-client RPC sequence numbers
      client includes "seen all replies <= X" with every RPC
      much like TCP sequence #s and acks
    or only allow client one outstanding RPC at a time
      arrival of seq+1 allows server to discard all <= seq
  how to handle dup req while original is still executing?
    server doesn't know reply yet
    idea: "pending" flag per executing RPC; wait or ignore


Go RPC is a simple form of "at-most-once"
  open TCP connection
  write request to TCP connection
  Go RPC never re-sends a request
    So server won't see duplicate requests
  Go RPC code returns an error if it doesn't get a reply
    perhaps after a timeout (from TCP)
    perhaps server didn't see request
    perhaps server processed request but server/net failed before reply came back

#Lab2

a) heartbeat period not two small, and leader recovery time not too large

GFS:
why client cache chunk handle & chunck server

snapshot and record append operations.
checkpoint in another thread, remained work use another log
log gap
version

###

Raft: log entry stage, commit stage
Time:
replicate log
committed
1) replicate log pass to follower, but not committed, leader crash,  client re-try request, 自动去重?
2） 

https://www.cnblogs.com/mindwind/p/5231986.html
http://thesecretlivesofdata.com/raft/ animation



While waiting for votes, a candidate may receive an
AppendEntries RPC from another server claiming to be
leader. If the leader’s term (included in its RPC) is at least
as large as the candidate’s current term, then the candidate
recognizes the leader as legitimate and returns to follower
state. If the term in the RPC is smaller than the candidate’s
current term, then the candidate rejects the RPC and continues
in candidate state


The leader appends the command to its log as a new entry, then issues
AppendEntries RPCs in parallel to each of the other
servers to replicate the entry. When the entry has been
safely replicated (as described below), the leader applies
the entry to its state machine and returns the result of that
execution to the client.
A log entry is committed once the leader
that created the entry has replicated it on a majority of
the servers (e.g., entry 7 in Figure 6). This also commits
all preceding entries in the leader’s log, including entries
created by previous leaders.

The RequestVote RPC implements this restriction: the RPC
includes information about the candidate’s log, and the
voter denies its vote if its own log is more up-to-date than
that of the candidate.

实际上raft协议在leader选举阶段，由于老leader可能也还存活，也会存在不只一个leader的情形，只是不存在term一样的两个leader，因为选举算法要求leader得到同一个term的多数派的同意，同时赞同的成员会承诺不接受term更小的任何消息。这样可以根据term大小来区分谁是合法的leader。

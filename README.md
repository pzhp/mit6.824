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

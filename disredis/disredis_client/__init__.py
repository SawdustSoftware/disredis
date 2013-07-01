"""
Distributed Redis Client

The DisredisClient class can be used in place of a StrictRedis client. Instead
of passing the host and port in, pass in a list of Sentinel addresses in
the form of "host:port". It will connect to the first responding Sentinel
and query it for masters that it knows about. These masters will become the
nodes that are sharded across. As long as the number of masters does not
change, the sharding will be stable even if there is a node failure.

Node failures are handled by asking the Sentinel for the updated master of
the given node. During the initial failure, some request may error out
with a ConnectionError if they are made between when the node fails and
when Sentinel executes the fail-over procedure.

"""
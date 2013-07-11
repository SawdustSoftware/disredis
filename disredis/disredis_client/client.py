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

import logging
from hashlib import sha1
from functools import wraps

from redis.client import StrictRedis
from redis.exceptions import ConnectionError


class Node(object):
    """
    Represents a single master node in the Redis cluster.
    """
    redis_client_class = StrictRedis

    def __init__(self, name, host, port):
        self.name = name
        self.host = host
        self.port = port
        logger = logging.getLogger('console')
        logger.info("Connecting to Redis master %s - %s:%s" % (name, host, port))
        self.connection = self.redis_client_class(host, int(port))


def executeOnNode(func):
    """
    Decorator that will cause the function to be executed on the proper
    redis node. In the case of a Connection failure, it will attempt to find
    a new master node and perform the action there.
    """
    @wraps(func)
    def wrapper(self, key, *args, **kwargs):
        node = self.get_node_for_key(key)
        nodeFunc = getattr(node.connection, func.__name__)
        try:
            return nodeFunc(key, *args, **kwargs)
        except ConnectionError:
            # if it fails a second time, then sentinel hasn't caught up, so
            # we have no choice but to fail for real.
            node = self.get_master(node)
            nodeFunc = getattr(node.connection, func.__name__)
            return nodeFunc(key, *args, **kwargs)
    return wrapper


class DisredisClient(object):
    """
    StrictRedis-compatible client object for a cluster of redis servers. The
    constructor takes a list of Sentinel addresses in the form of "host:port".
    Redis master nodes will be obtained from the Sentinels.
    """
    redis_client_class = StrictRedis
    sentinel = None
    nodes = None

    def __init__(self, sentinel_addresses):
        self.sentinel_addresses = sentinel_addresses
        self._get_nodes()

    def _connect(self):
        """
        Connect to a sentinel, accounting for sentinels that fail.
        """
        while True:
            try:
                address = self.sentinel_addresses.pop(0)
                logger = logging.getLogger('custommade_logging')
                logger.info("Connecting to Sentinel %s" % address)
                host, port = address.split(":")
                self.sentinel = self.redis_client_class(host, int(port))
                self.sentinel_addresses.append(address)
                break
            except ConnectionError:
                if not self.sentinel_addresses:
                    raise
            except IndexError:
                raise ConnectionError("Out of available Sentinel addresses!")

    def _execute_sentinel_command(self, *args, **kwargs):
        """
        Run a command on a sentinel, but fail over to the next sentinel in the
        list if there's a connection problem.
        """
        while True:
            try:
                if self.sentinel is None:
                    self._connect()
                return self.sentinel.execute_command("SENTINEL", *args,
                    **kwargs)
            except ConnectionError:
                self.sentinel = None
                if self.sentinel_addresses:
                    self.sentinel_addresses.pop()  # pull the current connection off
                else:
                    raise

    def _get_nodes(self):
        """
        Retrieve the list of nodes and their masters from the Sentinel server.
        """
        masterList = self._execute_sentinel_command("MASTERS")
        self.nodes = []
        for master in masterList:
            info = dict(zip(master[::2], master[1::2]))
            self.nodes.append(Node(info["name"], info["ip"], info["port"]))

    def get_master(self, node):
        """
        Returns the current master for a node. If it's different from the
        passed in node, update our node list accordingly.
        """
        host, port = self._execute_sentinel_command("get-master-addr-by-name",
            node.name)
        if host == node.host and port == node.port:
            return node
        newNode = Node(node.name, host, port)
        self.nodes[self.nodes.index(node)] = newNode
        return newNode

    def get_node_for_key(self, key):
        """
        Returns a node for the given key. Keys with {} in them will be sharded
        based on only the string between the brackets. This is for future
        compatibility with Redis Cluster (and is also a nice feature to have).
        """
        if "{" in key and "}" in key:
            key = key[key.index("{") + 1: key.index("}")]
        return self.nodes[int(sha1(key).hexdigest(), 16) % len(self.nodes)]

    # The remainder of this class is implementing the StrictRedis interface.
    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        raise NotImplementedError("Not supported for disredis.")

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        raise NotImplementedError("Not supported for disredis.")

    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single arguement which is a Pipeline object.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def lock(self, name, timeout=None, sleep=0.1):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.
        """

    @executeOnNode  # use the shard_hint for the key.
    def pubsub(self, shard_hint=None):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """

    #### SERVER INFORMATION ####
    def bgrewriteaof(self):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        raise NotImplementedError("Not supported for disredis.")

    def bgsave(self):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        raise NotImplementedError("Not supported for disredis.")

    def client_kill(self, address):
        "Disconnects the client at ``address`` (ip:port)"
        raise NotImplementedError("Not supported for disredis.")

    def client_list(self):
        "Returns a list of currently connected clients"
        raise NotImplementedError("Not supported for disredis.")

    def client_getname(self):
        "Returns the current connection name"
        raise NotImplementedError("Not supported for disredis.")

    def client_setname(self, name):
        "Sets the current connection name"
        raise NotImplementedError("Not supported for disredis.")

    def config_get(self, pattern="*"):
        "Return a dictionary of configuration based on the ``pattern``"
        raise NotImplementedError("Not supported for disredis.")

    def config_set(self, name, value):
        "Set config item ``name`` with ``value``"
        raise NotImplementedError("Not supported for disredis.")

    def dbsize(self):
        "Returns the number of keys in the current database"
        raise NotImplementedError("Not supported for disredis.")

    def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def debug_object(self, key):
        "Returns version specific metainformation about a give key"

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        # TODO support this.
        return self.execute_command('DEL', *names)
    __delitem__ = delete

    def echo(self, value):
        "Echo the string back from the server"
        raise NotImplementedError("Not supported for disredis.")

    def flushall(self):
        "Delete all keys in all databases on the current host"
        raise NotImplementedError("Not supported for disredis.")

    def flushdb(self):
        "Delete all keys in the current database"
        raise NotImplementedError("Not supported for disredis.")

    def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        raise NotImplementedError("Not supported for disredis.")

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        raise NotImplementedError("Not supported for disredis.")

    def object(self, infotype, key):
        "Return the encoding, idletime, or refcount about the key"
        raise NotImplementedError("Not supported for disredis.")

    def ping(self):
        "Ping the Redis server"
        raise NotImplementedError("Not supported for disredis.")

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        raise NotImplementedError("Not supported for disredis.")

    def shutdown(self):
        "Shutdown the server"
        raise NotImplementedError("Not supported for disredis.")

    def slaveof(self, host=None, port=None):
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguements, the
        instance is promoted to a master instead.
        """
        raise NotImplementedError("Not supported for disredis.")

    #### BASIC KEY COMMANDS ####
    @executeOnNode
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """

    @executeOnNode
    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """

    @executeOnNode
    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """

    @executeOnNode
    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
    __contains__ = exists

    @executeOnNode
    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """

    @executeOnNode
    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """

    @executeOnNode
    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value:
            return value
        raise KeyError(name)

    @executeOnNode
    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"

    @executeOnNode
    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """

    @executeOnNode
    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """

        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.incr(name, amount)

    @executeOnNode
    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        raise NotImplementedError("Not supported for disredis.")

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        raise NotImplementedError("Not supported for disredis.")

    def mset(self, mapping):
        "Sets each key in the ``mapping`` dict to its corresponding value"
        raise NotImplementedError("Not supported for disredis.")

    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"

    @executeOnNode
    def persist(self, name):
        "Removes an expiration on ``name``"

    @executeOnNode
    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """

    @executeOnNode
    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """

    @executeOnNode
    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """

    @executeOnNode
    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"

    def randomkey(self):
        "Returns the name of a random key"
        raise NotImplementedError("Not supported for disredis.")

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        raise NotImplementedError("Not supported for disredis.")

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
    __setitem__ = set

    @executeOnNode
    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """

    @executeOnNode
    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """

    @executeOnNode
    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"

    @executeOnNode
    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """

    @executeOnNode
    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"

    @executeOnNode
    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """

    @executeOnNode
    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"

    @executeOnNode
    def type(self, name):
        "Returns the type of key ``name``"

    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        raise NotImplementedError("Not supported for disredis.")

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        raise NotImplementedError("Not supported for disredis.")

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """

    @executeOnNode
    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """

    @executeOnNode
    def llen(self, name):
        "Return the length of the list ``name``"

    @executeOnNode
    def lpop(self, name):
        "Remove and return the first item of the list ``name``"

    @executeOnNode
    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"

    @executeOnNode
    def lpushx(self, name, value):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"

    @executeOnNode
    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """

    @executeOnNode
    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """

    @executeOnNode
    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"

    @executeOnNode
    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """

    @executeOnNode
    def rpop(self, name):
        "Remove and return the last item of the list ``name``"

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"

    @executeOnNode
    def rpushx(self, name, value):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"

    @executeOnNode
    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """

    #### SET COMMANDS ####
    @executeOnNode
    def sadd(self, name, *values):
        "Add ``value(s)`` to set ``name``"

    @executeOnNode
    def scard(self, name):
        "Return the number of elements in set ``name``"

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        raise NotImplementedError("Not supported for disredis.")

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        raise NotImplementedError("Not supported for disredis.")

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        raise NotImplementedError("Not supported for disredis.")

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"

    @executeOnNode
    def smembers(self, name):
        "Return all members of the set ``name``"

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.execute_command('SMOVE', src, dst, value)

    @executeOnNode
    def spop(self, name):
        "Remove and return a random member of set ``name``"

    @executeOnNode
    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """

    @executeOnNode
    def srem(self, name, *values):
        "Remove ``values`` from set ``name``"

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        raise NotImplementedError("Not supported for disredis.")

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        raise NotImplementedError("Not supported for disredis.")

    #### SORTED SET COMMANDS ####
    @executeOnNode
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """

    @executeOnNode
    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"

    @executeOnNode
    def zcount(self, name, min, max):
        """
        """

    @executeOnNode
    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        raise NotImplementedError("Not supported for disredis.")

    @executeOnNode
    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """

    @executeOnNode
    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """

    @executeOnNode
    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """

    @executeOnNode
    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"

    @executeOnNode
    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """

    @executeOnNode
    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """

    @executeOnNode
    def zrevrange(self, name, start, num, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``num`` sorted in descending order.

        ``start`` and ``num`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """

    @executeOnNode
    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """

    @executeOnNode
    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """

    @executeOnNode
    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        raise NotImplementedError("Not supported for disredis.")

    #### HASH COMMANDS ####
    @executeOnNode
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"

    @executeOnNode
    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"

    @executeOnNode
    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"

    @executeOnNode
    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"

    @executeOnNode
    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"

    @executeOnNode
    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """

    @executeOnNode
    def hkeys(self, name):
        "Return the list of keys within hash ``name``"

    @executeOnNode
    def hlen(self, name):
        "Return the number of elements in hash ``name``"

    @executeOnNode
    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """

    @executeOnNode
    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """

    @executeOnNode
    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """

    @executeOnNode
    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"

    @executeOnNode
    def hvals(self, name):
        "Return the list of values within hash ``name``"

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        raise NotImplementedError("Not supported for disredis.")

    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the LUA ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        raise NotImplementedError("Not supported for disredis.")

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a LUA script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        raise NotImplementedError("Not supported for disredis.")

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        raise NotImplementedError("Not supported for disredis.")

    def script_flush(self):
        "Flush all scripts from the script cache"
        raise NotImplementedError("Not supported for disredis.")

    def script_kill(self):
        "Kill the currently executing LUA script"
        raise NotImplementedError("Not supported for disredis.")

    def script_load(self, script):
        "Load a LUA ``script`` into the script cache. Returns the SHA."
        raise NotImplementedError("Not supported for disredis.")

    def register_script(self, script):
        """
        Register a LUA ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with LUA scripts.
        """
        raise NotImplementedError("Not supported for disredis.")

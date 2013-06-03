"""
Tests for disredis, a clustered Redis client.

"""
from unittest import TestCase

from redis.exceptions import ConnectionError

from disredis.client import DisredisClient, Node

class MockStrictRedis(object):
    """
    A mock version of a redis client connection. Used for both normal Redis
    and Sentinel servers.
    """
    fail = False

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = {}
        self.masters = [["name", "node1", "ip", "1.2.3.4", "port", "1"],
                ["name", "node2", "ip", "1.2.3.4", "port", "2"]]

    def get(self, key):
        if self.fail:
            raise ConnectionError("FAIL!")
        return self.data[key]

    def set(self, key, value):
        if self.fail:
            raise ConnectionError("FAIL!")
        self.data[key] = value

    def execute_command(self, command, sub_command, *args):
        if self.fail:
            raise ConnectionError("FAIL!")
        assert command == "SENTINEL"
        if sub_command == "MASTERS":
            return self.masters
        if sub_command == "get-master-addr-by-name":
            for master in self.masters:
                if master[1] == args[0]:
                    return master[3], master[5]
            raise KeyError("Unknown master %s" % args[0])
        raise KeyError("Sentinel got command %s" % sub_command)


class TestDisredisClient(TestCase):
    """
    Unit tests for the DisredisClient class. These tests use two mock sentinels
    which monitor two mock Redis nodes.
    """
    def setUp(self):
        self.old_client = DisredisClient.redis_client_class
        DisredisClient.redis_client_class = MockStrictRedis
        Node.redis_client_class = MockStrictRedis
        self.client = DisredisClient(["127.0.0.1:6383", "127.0.0.1:6384"])

    def tearDown(self):
        DisredisClient.redis_client_class = self.old_client
        Node.redis_client_class = self.old_client

    def test_get_nodes(self):
        """
        get_nodes is called from __init__, so check that the nodes were added.
        """
        self.assertEqual(len(self.client.nodes), 2)
        self.assertEqual(self.client.nodes[0].name, "node1")
        self.assertEqual(self.client.nodes[1].name, "node2")
        self.assertEqual(self.client.nodes[0].host, "1.2.3.4")
        self.assertEqual(self.client.nodes[1].host, "1.2.3.4")
        self.assertEqual(self.client.nodes[0].port, "1")
        self.assertEqual(self.client.nodes[1].port, "2")

    def test_set_get(self):
        """
        Test setting a key on the master and retrieving it.
        """
        self.client.set("test", "foo")
        self.assertEqual(self.client.get("test"), "foo")
        self.assertEqual(self.client.nodes[0].connection.data, {})
        self.assertEqual(self.client.nodes[1].connection.data, {"test":"foo"})

    def test_set_get_failover(self):
        """
        If a connection gives a ConnectionError, switch to the backup.
        """
        failed = self.client.nodes[1] 
        failed.connection.fail = True
        self.client.sentinel.masters[1] = ["name", "node2", "ip", "1.2.3.4",
            "port", "11"]        
        self.client.set("test", "foo")
        self.assertNotEqual(failed, self.client.nodes[1])
        self.assertEqual(self.client.get("test"), "foo")
        self.assertEqual(self.client.nodes[0].connection.data, {})
        self.assertEqual(self.client.nodes[1].connection.data, {"test":"foo"})

    def test_set_with_specified_key(self):
        """
        Enclosing a part of the key in {} will cause it to be used as the
        sharding key. This is future-compatible with Redis Sentinel.
        """
        self.client.set("test{1}", "foo")
        self.assertEqual(self.client.get("test{1}"), "foo")
        self.assertEqual(self.client.nodes[0].connection.data, {})
        self.assertEqual(self.client.nodes[1].connection.data, {"test{1}":"foo"})
        self.client.set("other{1}", "bar")
        self.assertEqual(self.client.get("other{1}"), "bar")
        self.assertEqual(self.client.nodes[0].connection.data, {})
        self.assertEqual(self.client.nodes[1].connection.data, {"test{1}":"foo",
            "other{1}":"bar"})
        self.client.set("test{2}", "baz")
        self.assertEqual(self.client.get("test{2}"), "baz")
        self.assertEqual(self.client.nodes[0].connection.data, {"test{2}":"baz"})
        self.assertEqual(self.client.nodes[1].connection.data, {"test{1}":"foo",
            "other{1}":"bar"})

    def test_get_master(self):
        """
        Returning the master of a node that isn't failed should return the
        same object.
        """
        self.assertEqual(self.client.get_master(self.client.nodes[0]),
            self.client.nodes[0])

    def test_get_master_different_port(self):
        """
        Returning the master of a node that is failed should return a new node
        and reset the one in the nodes list.
        """
        self.client.sentinel.masters[0] = ["name", "node1", "ip", "1.2.3.4",
            "port", "11"]
        node = self.client.nodes[0]
        master = self.client.get_master(node)
        self.assertNotEqual(node, master)
        self.assertEqual(master, self.client.nodes[0])
        self.assertEqual(master.port, "11")

    def test_get_master_different_host(self):
        """
        Returning the master of a node that is failed should return a new node
        and reset the one in the nodes list.
        """
        self.client.sentinel.masters[0] = ["name", "node1", "ip", "2.2.3.4",
            "port", "1"]
        node = self.client.nodes[0]
        master = self.client.get_master(node)
        self.assertNotEqual(node, master)
        self.assertEqual(master, self.client.nodes[0])
        self.assertEqual(master.host, "2.2.3.4")

    def test_get_master_failed_sentinel(self):
        """
        Any time a sentinel fails, the client should use the next one in line.
        """
        self.client.sentinel.fail = True
        self.assertEqual(self.client.get_master(self.client.nodes[0]),
            self.client.nodes[0])

    def test_get_master_failed_sentinels(self):
        """
        If all sentinels fail, a ConnectionError is raised.
        """
        MockStrictRedis.fail = True
        try:
            self.assertRaises(ConnectionError, self.client.get_master,
                self.client.nodes[0])
        finally:
            MockStrictRedis.fail = False            

    def test_get_node_for_key(self):
        """
        This function uses an sha1 hash to distribute keys across available
        nodes.
        """
        self.assertEqual(self.client.get_node_for_key("1"),
            self.client.nodes[1])
        self.assertEqual(self.client.get_node_for_key("2"),
            self.client.nodes[0])
        self.assertEqual(self.client.get_node_for_key("3"),
            self.client.nodes[1])

    def test_unavailable_api(self):
        """
        Api calls that aren't tied to a single key aren't available.
        """
        self.assertRaises(NotImplementedError, self.client.time)


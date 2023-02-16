import sys
import time
from collections import Counter

import redis
import json

import ray

import os

@ray.remote
def get_host_name(x):
    import platform
    import time

    time.sleep(0.01)
    return x + (platform.node(),)

@ray.remote
def send_to_redis(record):
    #todo


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print(
                "{} nodes have joined so far, waiting for {} more.".format(
                    num_nodes, expected - num_nodes
                )
            )
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    #https://realpython.com/python-redis/
    wait_for_nodes(4)

    r = redis.Redis(host=os.environ["REDIS_ENDPOINT"])

    #loading from file
    #making database available for actros
    f = open('../subtree/subtrees1.json')
    data = json.load(f)  # OCID->[OCID]
    f.close()

    database_object_ref = ray.put(data)

    # Check that objects can be transferred from each node to each other node.
    for i in range(10):
        print("Iteration {}".format(i))
        results = [get_host_name.remote(get_host_name.remote(())) for _ in range(100)]
        print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()
    time.sleep(20)


if __name__ == "__main__":
    ray.init(address="localhost:6379")
    main()
import sys
import time
from collections import Counter

#from redisearch import Client
import redis
import json
import ray
import os

ray.init()

f = open(
    '/Users/tiagoooliveira/Documents/dev/scala/akka-http-quickstart-scala/src/main/resources/patent-13062022-1.json')
data = json.load(f)
f.close()

database_object_ref = ray.put(data)

@ray.remote
def get_host_name(x):
    import platform
    import time

    time.sleep(0.01)
    return x + (platform.node(),)

@ray.remote
def send_to_redis(record):
    print("sending")
    #https://pypi.org/project/redisearch/

@ray.remote
def retrieve_task(item):
    obj_store_data = ray.get(database_object_ref)
    time.sleep(item / 10.)
    return item, obj_store_data[item]

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

def print_runtime(input_data, start_time, decimals=1):
    print(f'Runtime: {time.time() - start_time:.{decimals}f} seconds, data:')
    print(*input_data, sep='\n')

def main():

    #https://realpython.com/python-redis/
    #wait_for_nodes(4)

    #curl -O --insecure -I https://files.pythonhosted.org/packages/ba/7d/2ae672b176e675519c0f5d2cb46f023e37be3754a59f7307756e3fdf7552/redisearch-2.1.1-py3-none-any.whl
    #r = redis.Redis(host=os.environ["REDIS_ENDPOINT"])

    #loading from file
    #making database available for actors
    #iterate over remote method/actor (responsible to send json to Redis) at the limit of actors available


    start = time.time()
    #data_references = [retrieve_task.remote(item) for item in range(8)]
    data_references = retrieve_task.remote("CN112310387B")
    all_data = []


    finished, data_references = ray.wait(data_references)
    data = ray.get(finished)

    print_runtime(data, start, 3)

    #all_data.extend(data)

    print("Success!")
    sys.stdout.flush()
    time.sleep(20)


if __name__ == "__main__":
    main()
import sys
import time
import logging
from collections import Counter

from fastapi import FastAPI, File, UploadFile, Request
import uvicorn
import shutil
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from redisearch import Client
import redisearch
import json
import ray
import os

app = FastAPI()
templates = Jinja2Templates(directory="templates")
@app.get("/upload/", response_class=HTMLResponse)
async def upload(request: Request):
   return templates.    TemplateResponse("uploadfile.html", {"request": request})

@app.post("/uploader/")
async def create_upload_file(file: UploadFile = File(...)):
   with open("destination.png", "wb") as buffer:
      shutil.copyfileobj(file.file, buffer)
   return {"filename": file.filename}

@ray.remote
class RecordTracker:
    def __int__(self):
        self.total = 0

    def inc(self):
        self.total += 1

    def counts(self):
        return self.total

file = open(
    '/Users/tiagoooliveira/Documents/dev/scala/akka-http-quickstart-scala/src/main/resources/patent-13062022-1.json')
data = json.load(file)
file.close()

database_object_ref = ray.put(data)

@ray.remote
def get_host_name(x):
    import platform
    import time

    time.sleep(0.01)
    return x + (platform.node(),)

@ray.remote
def f(x):
    time.sleep(1)
    return x

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
    print("initialized go")
    # Start 4 tasks in parallel.
    result_ids = []
    for i in range(4):
        result_ids.append(f.remote(i))

    # Wait for the tasks to complete and retrieve the results.
    # With at least 4 cores, this will take 1 second.
    results = ray.get(result_ids)  # [0, 1, 2, 3]

    #https://realpython.com/python-redis/
    #wait_for_nodes(4)

    #curl -O --insecure -I https://files.pythonhosted.org/packages/ba/7d/2ae672b176e675519c0f5d2cb46f023e37be3754a59f7307756e3fdf7552/redisearch-2.1.1-py3-none-any.whl
    #r = redis.Redis(host=os.environ["REDIS_ENDPOINT"])

    #loading from file
    #making database available for actors
    #iterate over remote method/actor (responsible to send json to Redis) at the limit of actors available





if __name__ == "__main__":
    if ray.is_initialized:
        ray.shutdown()
    ray.init(logging_level=logging.ERROR)

    start = time.time()
    # data_references = [retrieve_task.remote(item) for item in range(8)]
    data_references = retrieve_task.remote("CN112310387B")
    all_data = []

    finished, data_references = ray.wait(data_references)
    data = ray.get(finished)

    print_runtime(data, start, 3)

    # all_data.extend(data)

    print("Success!")
    sys.stdout.flush()
    time.sleep(20)
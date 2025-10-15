import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import queue
import time


class osrm_handler():
    def __init__(self, base_url = None):
        if base_url:
            self.base_url = base_url
        else: # implied None
            self.base_url = 'http://127.0.0.1:5000/'
        self.nearest_url = self.base_url + 'nearest/v1/driving/'
        self.route_url = self.base_url + 'route/v1/driving/'

        # Set up the session
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount('http://', adapter)

    
    def get_nearest_node(self, latlon, end=False):
        lat, lon = latlon

        request = self.nearest_url + f"{lon},{lat}?generate_hints=false&number=1"
        r = None
        num = 0
        while r is None:
            try:
                r = self.session.get(request)
            except Exception as e:
                print(e)
                num += 1
                time.sleep(1)
                if num > 10:
                    raise
        r = r.json()

        if r['code'] != "Ok":
            print(f'No node found at ({lat},{lon})')
            return -1, -1 # TODO: Fix this to raise an actual error

        # Grab the node and return
        if end:
            node = int(r["waypoints"][0]["nodes"][1])
        else:
            node = int(r["waypoints"][0]["nodes"][0])  # first node only
        result_lonlat = r["waypoints"][0]["location"]
        result_latlon = (result_lonlat[1], result_lonlat[0])
        return (node, result_latlon)


    def get_route_nodes(self, latlon_start, latlon_end):
        request = self.route_url + \
            f"{latlon_start[1]},{latlon_start[0]};" + \
            f"{latlon_end[1]},{latlon_end[0]}?" + \
            "generate_hints=false&annotations=nodes"

        r = None
        num = 0
        while r is None:
            try:
                r = self.session.get(request)
            except Exception as e:
                print(e)
                num += 1
                time.sleep(0.1)
                if num > 10:
                    raise
        r = r.json()

        if r['code'] != "Ok":
            print(f'Route not found! {r['code']}')
            return [] # TODO: Fix this to raise an actual error

        # Gather the node list
        return r['routes'][0]['legs'][0]['annotation']['nodes']
    
    def get_route_nodes_and_geometry(self, latlon_start, latlon_end):
        request = self.route_url + \
            f"{latlon_start[1]},{latlon_start[0]};" + \
            f"{latlon_end[1]},{latlon_end[0]}?" + \
            "generate_hints=false&annotations=nodes&overview=full&geometries=geojson"

        r = None
        num = 0
        while r is None:
            try:
                r = self.session.get(request)
            except Exception as e:
                print(e)
                num += 1
                time.sleep(0.1)
                if num > 10:
                    raise
        r = r.json()

        if r['code'] != "Ok":
            print(f'Route not found! {r['code']}')
            return [] # TODO: Fix this to raise an actual error
        
        # Gather the geometry and nodes from the reply
        lonlat_list = r['routes'][0]['geometry']['coordinates']
        nodes_list = r['routes'][0]['legs'][0]['annotation']['nodes']
        assert len(lonlat_list) == len(nodes_list), "return data is of different lengths!!!"
        latlon_dict = {nodes_list[i]:lonlat_list[i] for i in range(len(lonlat_list))}
    
        return nodes_list, latlon_dict

    def get_nearest_node_parallel(self, latlon_list, num_parallel=10):
        # Copy self into new items and add to a queue
        handler_queue = queue.Queue()
        for _ in range(num_parallel):
            handler_queue.put(osrm_handler(base_url=self.base_url))
        
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallel) as executor:
            futures = [executor.submit(_get_nearest_node_helper, (latlon, handler_queue)) for latlon in latlon_list]
            result_nodes = [fut.result() for fut in futures] # Blocks
        
        # Clean up handlers
        while not handler_queue.empty:
            try:
                _ = handler_queue.get_nowait()
            except queue.Empty:
                pass
        
        return result_nodes

def _get_nearest_node_helper(args):
    latlon = args[0]
    handler_queue = args[1]
    handler = handler_queue.get()
    result = handler.get_nearest_node(latlon)
    handler_queue.put(handler)
    return result

def _test_parallel():
    import numpy as np
    num_requests = 10000
    num_parallel = 50
    lat_starts = 47.640145 + (np.arange(num_requests) - 500) / 10000
    lon_starts = -122.100282 + (np.arange(num_requests) - 500) / 10000
    latlon_starts = [(float(lat), float(lon)) for lat, lon in zip(lat_starts, lon_starts)]

    handler = osrm_handler()
    print("Starting parallel requests")
    start = time.time()
    nodes = handler.get_nearest_node_parallel(latlon_starts, num_parallel=num_parallel)
    end = time.time()
    print(f"Parallel elapsed: {end - start} seconds @ {(end - start) / num_requests}s per request")

    start = time.time()
    _ = [handler.get_nearest_node(latlon) for latlon in latlon_starts]
    end = time.time()
    print(f"Sequential elapsed: {end - start} seconds @ {(end - start) / num_requests}s per request")



if __name__ == "__main__":
    _test_parallel()


    # latlon_start = (47.640145, -122.100282)
    # latlon_end = (47.635639, -122.105031)

    # handler = osrm_handler()

    # # Try getting nodes
    # node_start, latlon_start = handler.get_nearest_node(latlon_start)
    # node_end, latlon_end = handler.get_nearest_node(latlon_end, end=True)

    # # Try getting a route
    # route_nodes = handler.get_route_nodes(latlon_start, latlon_end)

    # print(f"From {node_start} to {node_end}")
    # print(route_nodes)
import numpy as np

from osrm_interface import osrm_handler


# Class to contain all the functionality
class map_anywhere():
    def __init__(self):
        # Grab the interface to osrm
        self.osrm_interface = osrm_handler() # Default is fine
        self.dest_node = 0
        self.dest_latlon = None
        self.bbox = None
        self.node_dict = {}
        self.segment_counts = {}
        self.lonlat_dict = {}


    def set_destination(self, dest_latlon):
        self.dest_node, self.dest_latlon = self.osrm_interface.get_nearest_node(dest_latlon, end=True)
    

    def set_bounding_box(self, latlon_nw, latlon_se):
        self.bbox = (latlon_nw, latlon_se)
    

    def set_bounding_box_centered(self, width_deg, height_deg):
        if self.dest_latlon is None:
            print("Must set destination first!")
            assert(False) # TODO: Change this to a real error
        
        # TODO: Add checks to not go out of bounds
        latlon_nw = (self.dest_latlon[0] + height_deg / 2, self.dest_latlon[1] - width_deg / 2)
        latlon_se = (self.dest_latlon[0] - height_deg / 2, self.dest_latlon[1] + width_deg / 2)
        self.set_bounding_box(latlon_nw, latlon_se)


    def sample_nodes(self, num_nodes):
        # Want to grid-sampe latlon. Ignore earth curvature
        h = self.bbox[0][0] - self.bbox[1][0]
        w = self.bbox[1][1] - self.bbox[0][1]
        step = np.sqrt(h * w / num_nodes)
        num_h = int(h / step)
        num_w = int(w / step)

        lats = np.arange(num_h) * step + step / 2 + self.bbox[1][0]
        lons = np.arange(num_w) * step + step / 2 + self.bbox[0][1]

        # Loop through lats + lons and get nearest nodes list
        self.node_dict = {}
        for lat in lats:
            for lon in lons:
                n, latlon = self.osrm_interface.get_nearest_node((lat, lon))
                if n > 0: # Not error
                    self.node_dict[n] = latlon


    def sample_routes(self, geometry=False):
        self.segment_counts = {} # Reset

        for node in self.node_dict:
            start_latlon = self.node_dict[node]

            if geometry:
                nodes, lonlat_dict = self.osrm_interface.get_route_nodes_and_geometry(start_latlon, self.dest_latlon)

                # Add the geometry to the saved dict. Just yolo it, no check for overwriting
                for n in lonlat_dict:
                    self.lonlat_dict[n] = lonlat_dict[n]
            else:
                nodes = self.osrm_interface.get_route_nodes(start_latlon, self.dest_latlon)

            # TODO: Undo this hack when OSRM fixes their bug
            nodes = [n for n in nodes if type(n) is int]

            if len(nodes) == 0:
                print(f"Warning! No route found! Start_loc: {node} @ latlon {start_latlon}")
                continue
            if nodes[-1] != self.dest_node:
                print(f"Warning! Destination node not at endpoint! {nodes[0]} to {nodes[-1]} (Not {self.dest_node})")

            for n1, n2 in zip(nodes[:-1], nodes[1:]):
                
                n1n2 = (int(n1), int(n2))
                if n1n2 in self.segment_counts:
                    self.segment_counts[n1n2] += 1
                else:
                    self.segment_counts[n1n2] = 1


class graph_node():
    def __init__(self, parents=None, children=None, weights=None):
        self.parents = [] if parents is None else parents
        self.children = [] if children is None else children
        self.weights = [] if weights is None else weights

    
    def __str__(self):
        return f"{self.parents} @ {self.weights} <-- {self.children}"

    def __repr__(self):
        return self.__str__()


def build_node_graph(node_pair_dict):
    # node_pair_dict --> A dict of tuples of start/end nodes with weight
    # Returns: A node graph with each note denoting parent/child connections, and the parent weight
    tree_dict = {}
    for (n1, n2), weight in node_pair_dict.items():
        # Add n1 child connection to the n2 node
        if n2 in tree_dict:
            tree_dict[n2].children.append(n1)
        else:
            tree_dict[n2] = graph_node(children = [n1,])

        
        # Add n2 as parent connection to the n1 entry
        if n1 in tree_dict:
            tree_dict[n1].parents.append(n2)
            tree_dict[n1].weights.append(weight)
        else:
            tree_dict[n1] = graph_node(parents = [n2,], weights = [weight])
    
    return tree_dict


# Removes node pairs which form a loop
def filter_tree(tree_dict):
    finished = False
    check_nodes = [n for n in tree_dict]
    while not finished:
        loop_nodes = []
        for n in check_nodes:
            for parent in tree_dict[n].parents:
                if parent in tree_dict[n].children:
                    loop_nodes.append(n)
                    break
        
        if len(loop_nodes) == 0:
            finished = True
        else:
            for n in loop_nodes:
                idxs = []
                lns = []
                weights = []
                for idx, (ln, w) in enumerate(zip(tree_dict[n].parents, tree_dict[n].weights)):
                    if ln in tree_dict[n].children:
                        idxs.append(idx)
                        lns.append(ln)
                        weights.append(w)
                
                # Loop was already resolved
                if len(idxs) == 0:
                    continue

                # Remove the smaller of the loops
                rm_weight = min(weights)
                rm_idx = weights.index(rm_weight)
                rm_node = lns[rm_idx]
                parent_idx = idxs[rm_idx]
                # print(f"Removing parent connection {rm_node} at index {parent_idx}")

                # Add power to parent node's parent connection
                grandparent_idx = tree_dict[rm_node].parents.index(n)
                tree_dict[rm_node].weights[grandparent_idx] += rm_weight

                # Remove parent connection
                _ = tree_dict[n].parents.pop(parent_idx)
                _ = tree_dict[n].weights.pop(parent_idx)
                grandparent_idx = tree_dict[rm_node].children.index(n)
                _ = tree_dict[rm_node].children.pop(grandparent_idx)
        
        # Reset the search list
        check_nodes = loop_nodes.copy()


# Finds a loop in the graph
def break_loops(tree_dict):
    # Just start at the first index and start going
    node_set = {k for k in tree_dict}
    revisit = []
    new_segments = [] # Need to put the splits here
    reset = True
    revisit_idx = None
    while len(node_set) > 0:
        if reset:
            curr_node = next(iter(node_set))
            visited = {curr_node}
            reset = False
        if curr_node in node_set:
            node_set.remove(curr_node)
        
        parents = tree_dict[curr_node].parents
        if revisit_idx is not None:
            next_node = parents[revisit_idx]
            revisit_idx = None
        elif len(parents) > 1: # multiparent
            revisit.append((curr_node, 1 if revisit_idx is None else revisit_idx + 1))
            next_node = parents[0]
            node_set.add(curr_node) # Wil revisit again
        elif len(parents) == 1:
            next_node = parents[0]
        else: # head node
            visited.add(curr_node)
            next_node = None

        # print(f"At node {curr_node}: {tree_dict[curr_node]}, next node: {next_node}")
        # Case where the parent node was already removed
        if next_node not in tree_dict:
            reset = True
        elif next_node in visited: # Loop detection
            print(f"Found a loop at node {next_node}")
            parent_idx = tree_dict[curr_node].parents.index(next_node)
            weight = tree_dict[curr_node].weights.pop(parent_idx)
            _ = tree_dict[curr_node].parents.pop(parent_idx)
            tree_dict[next_node].children.remove(curr_node)
            new_segments.append((weight, [curr_node, next_node]))
            reset = True
        else: # Continue traverse
            curr_node = next_node
        visited.add(curr_node)

        # If reset, and revisit is full, grab that as the next node
        if reset and len(revisit) > 0:
            curr_node, revisit_idx = revisit.pop()
            num_parents = len(tree_dict[curr_node].parents)
            if revisit_idx > num_parents - 1:
                revisit_idx = 0 # Not sure if this will break things
            if num_parents > 0:
                reset = False

    return new_segments
            




def extract_segments(tree_dict):
    segments = [] # Will be filled with (weight, [nodes,])

    # Implicit assumption: Each tree has at least one head
    head_nodes = [n for n in tree_dict if len(tree_dict[n].parents) == 0]

    while len(head_nodes) > 0:
        head = head_nodes.pop()
        head_node = tree_dict[head]

        # Traverse each child to form a path
        for child in head_node.children:
            child_node = tree_dict[child]

            # Get the weight. Special case of multi-parent node
            if len(child_node.parents) > 1:
                parent_idx = child_node.parents.index(head)
                weight = child_node.weights[parent_idx]
            else:
                weight = child_node.weights[0]

            segment = (weight, [head, child])            

            parent = head
            # Traverse down a single-path section of the tree
            while (len(child_node.children) == 1) and \
                  (tree_dict[child_node.children[0]].weights[0] == weight) and \
                  (len(child_node.parents) == 1):
                parent = child
                parent_node = child_node

                child = child_node.children[0]
                child_node = tree_dict[child_node.children[0]]
                segment[1].append(child)

                # Remove parent node from the tree, if not the head
                if parent != head:
                    del tree_dict[parent]

                # Check if a leaf node
                if len(child_node.children) == 0:
                    break
            
            segments.append(segment)
            # print(f"Reached end of traverse from {head} to {child}")
            # print(f"State: w: {weight}")
            # print(f"Child node: {child_node}")
            # print(f"Child Child: {tree_dict[child_node.children[0]]}")
            # print(f"Child is a leaf: {len(child_node.children) == 0}")
            # print(f"Child is a leaf: {len(child_node.children) == 0}")
            # print(f"child is multichild: {len(child_node.children) > 1}")
            # print(f"Child child is multiparent: {len(tree_dict[child_node.children[0]].parents) == 1}")
            # print(f"Child is midpoint: {tree_dict[child_node.children[0]].weights[0] != weight}")

            # Handle the child node
            if len(child_node.children) == 0: # Leaf
                if len(child_node.parents) > 1: # Just remove the parent connection
                    child_node.parents.remove(parent)
                else:
                    del tree_dict[child]
            elif len(child_node.children) > 1: # Multi-child
                if len(child_node.parents) > 1: # Also multi-parent
                    parent_idx = child_node.parents.index(parent)
                    _ = child_node.parents.pop(parent_idx)
                    _ = child_node.weights.pop(parent_idx)
                else: # New head
                    _ = child_node.parents.pop()
                    _ = child_node.weights.pop()
                    head_nodes.append(child)
            elif len(child_node.parents) > 1: # multi-parent
                parent_idx = child_node.parents.index(parent)
                _ = child_node.parents.pop(parent_idx)
                _ = child_node.weights.pop(parent_idx)
            elif tree_dict[child_node.children[0]].weights[0] != weight: # midpoint
                _ = child_node.parents.pop()
                _ = child_node.weights.pop()
                head_nodes.append(child)
        
        # Delete head after all children are traversed
        del tree_dict[head]
    
    # Check if tree is empty (it should be)
    if len(tree_dict) > 0:
        print(f"Tree is not empty!!!")
    
    return segments



if __name__ == "__main__":
    # Simple example
    dest_latlon = (47.635639, -122.105031)

    router = map_anywhere()
    router.set_destination(dest_latlon)
    router.set_bounding_box_centered(0.054882, 0.054882)
    router.sample_nodes(100)
    router.sample_routes()

    print(len(router.node_dict))
    print(len(router.segment_counts))
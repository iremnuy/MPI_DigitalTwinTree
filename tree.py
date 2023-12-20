from mpi4py import MPI

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

MASTER = 0

# Function to read and process the input file
def read_input_file(filename):
    with open(filename, 'r') as file:
        # Read the lines from the file and remove leading/trailing whitespaces
        lines = [line.strip() for line in file.readlines()]

    return lines

# Function to perform operations for each node
def perform_node_operations(node_info, product,num_children):
    # Extract relevant information from node_info dictionary
    machine_id = node_info["machine_id"]
    parent_id = node_info["parent_id"]
    initial_operation = node_info["initial_operation"]
    operations = node_info["operations"]

    # Perform some dummy operation for demonstration

    result = f"Result from machine {machine_id}"
    #for real result perform the current operation without adding,leaf nodes do not add 

    # Send the result back to the parent node, if not the root node
    if machine_id != 1:
        comm.send((result, machine_id), dest=parent_id)

    # Receive a result from a child node, if not a leaf node
    if machine_id not in leaf_nodes:
        for _ in range(num_children[machine_id]):
            result, child_id = comm.recv(source=MPI.ANY_SOURCE) #from any source (NOT FROM MASTER)
            print(f"Machine {machine_id} received result from machine {child_id}: {result}")
            #PERFORM THE OPERATION 
            #take each child result first add them with add function then perform the current opertion for this node

            # Send the result to the parent node, if not the root node
            if machine_id != 1:
                comm.send((result, machine_id), dest=parent_id)

# Master process
num_children = {}                
if rank == MASTER:
    # Read and process the input file
    input_lines = read_input_file("input.txt")

    # Extract relevant information from input_lines
    num_machines = int(input_lines[0])
    num_cycles = int(input_lines[1])
    wear_factors = list(map(int, input_lines[2].split()))
    maintenance_threshold = int(input_lines[3])

    # Process child-parent relationships to identify leaf nodes
    child_parent_relationships = [list(map(int, line.split())) for line in input_lines[4:num_machines+2]]
    parent_set = set()

    # Initialize worker information for each leaf node
    node_info = {}
    num_children = {i: 0 for i in range(1, num_machines + 1)}
    for child, parent, operation_name in child_parent_relationships:
        parent_set.add(parent)
        operations=[]
        if (child % 2 == 0) :
            operations : ["enhance","split","chop"]

        else :
            operations : ["trim","reverse"]
            
        node_info[child] = {
            "machine_id": child,
            "parent_id": parent,
            "initial_operation": operation_name,
            "operations": operations
            }
            # Add any other relevant information
            
        num_children[parent] += 1 #index i holds the number of children of node i

    # Determine leaf nodes (machines without parents)
    leaf_nodes = sorted(set(range(2, num_machines + 1)) - parent_set)

    # Extract initial product names
    num_leaf_machines = len(leaf_nodes)
    products = input_lines[num_machines+3:num_machines+3+num_leaf_machines-1] # Assuming line number is the same as num_leaf_machines

    # Distribute necessary information to worker processes
    for leaf_id, product in zip(leaf_nodes, products):
        # Send worker_info and product to the corresponding worker process
        comm.send((node_info[leaf_id], product), dest=leaf_id)

    # Receive the final result from the root node (ID 1)
    final_result, _ = comm.recv(source=1)
    print("Final Result:", final_result)

# Worker processes
else:
    # Receive information from the master process
    node_info, product = comm.recv(source=MASTER) #from master process to worker process

    # Perform operations for each node
    perform_node_operations(node_info, product,num_children) #Each leaf node initates its own chain of flow 

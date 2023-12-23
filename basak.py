from mpi4py import MPI

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
wear_opname = ["enhance", "reverse", "chop", "trim", "split"]
MASTER = 0


# Function to read and process the input file
def read_input_file(filename):
    with open(filename, 'r') as file:
        # Read the lines from the file and remove leading/trailing whitespaces
        lines = [line.strip() for line in file.readlines()]

    return lines


# Function to perform operations for each node
def calculate_string(product, operation, mod):
    print("calculate string this is the product", product, "this is the operation", operation, "this is the mod", mod)
    # NOTES
    # Take the threshold, calculate the weariness, do the current operation and update the index for the next call
    # if the threshold is exceeded, send the result to the parent node
    if operation == "enhance":
        # duplicate the first and last letters
        product = product[0] + product + product[-1]
    elif operation == "reverse":
        product = product[::-1]
    elif operation == "chop":
        if (len(product) > 1):
            product = product[:-1]
    elif operation == "trim":
        if (len(product) > 2):
            product = product[1:-1]
    elif operation == "split":
        length = len(product)
        if length % 2 == 0:
            split_point = length // 2
        else:
            split_point = (length + 1) // 2
        product = product[:split_point]
        print("split result", product)
    else:
        print("Invalid operation")

    print("OPERATION RESULT", product)
    return product


# Read and process the input file
input_lines = read_input_file("input.txt")

# Extract relevant information from input_lines
num_machines = int(input_lines[0])
num_cycles = int(input_lines[1])
wear_factors = list(map(int, input_lines[2].split()))
maintenance_threshold = int(input_lines[3])

# Process child-parent relationships to identify leaf nodes
child_parent_operations = [list(map(str, line.split())) for line in input_lines[4:num_machines + 3]]

# Initialize worker information for each leaf node
node_info = {}
num_children = {i: 0 for i in range(1, num_machines + 1)}
parent_set = set()
for child, parent, operation_name in child_parent_operations:
    parent = int(parent)
    child = int(child)
    parent_set.add(parent)
    operations = [operation_name]
    mod = 1  # Assuming one operation in the list

    node_info[child] = {
        "machine_id": child,
        "parent_id": parent,
        "initial_operation": operation_name,
        "operations": operations,
        "modulo": mod,
        "children_product": {}  # dictionary of children id and their results
    }

    # find initial operation index in the operations
    current_op_index = 0  # Index for the first operation in the list
    node_info[child]["current_op_number"] = current_op_index

    num_children[parent] += 1  # index i holds the number of children of node i
    
    if (int(parent)!= 1):
        node_info[parent]["children_product"][child] = 1 


# Determine leaf nodes (machines without parents)
leaf_nodes = sorted(set(range(2, num_machines + 1)) - parent_set)
print("leaf nodes", leaf_nodes)

# Extract initial product names
num_leaf_machines = len(leaf_nodes)
products = input_lines[num_machines + 3:num_machines + 3 + num_leaf_machines]  # Assuming line number is the same as num_leaf_machines
print("products", products)
if rank == MASTER:
    # Distribute necessary information to worker processes
    for i in range(1, size):
        if i <= len(leaf_nodes):
            machine_id = leaf_nodes[i - 1]
            initial_product = products[i - 1]
            print(f"Sending initial information to worker {leaf_nodes[i - 1]} - Machine ID: {machine_id}, node info: {node_info[machine_id]}")
            comm.send((machine_id, initial_product, node_info[machine_id]), dest=leaf_nodes[i - 1])

    # Identify and distribute information to the remaining non-leaf nodes
    for node_id, node_data in node_info.items():
        if node_id not in leaf_nodes:
            machine_id = node_id
            initial_product = None  # Adjust as needed
            print(f"Sending initial information to worker for non-leaf node {node_id} - Machine ID: {machine_id}, node info: {node_data}")
            comm.send((machine_id, initial_product, node_data), dest=node_id)

    # Collect results from worker processes
    final_machine_id, final_result = comm.recv()
    #for i in range(1, size):
    #    if i <= len(leaf_nodes):
    #        result = comm.recv(source=i)
    #        if result is not None:
    #            machine_id, result = result
    #            print(f"Received result from worker {i} - Machine ID: {machine_id}, Result: {result}")
    #            final_result += result  # Accumulate the result
    #        else:
    #            print(f"Received completion signal from worker {i}")



    print("Final Result:", final_result)

    # Additional processing after all workers have completed
    # ...

# Worker processes
else:
    # Receive information from master process
    machine_id, initial_product, node_info_local = comm.recv(source=MASTER)
    print(f"Worker {rank} - Received initial information - Machine ID: {machine_id}, local worker info: {node_info_local}")

    # Perform operations for the specified number of cycles
    for cycle in range(num_cycles):
        # Only collect results from children if the initial product is None (not a leaf)
        if initial_product is not None:
            # If initial product is not None, directly perform the operation and send the result to the parent bc we are leaf
            current_product = calculate_string(initial_product, node_info_local["operations"][0], node_info_local["modulo"])
            print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][0]}, Result: {current_product}")
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            print("LEAF IS SENDING THIS", (machine_id, current_product))
            comm.Barrier()

        else:
            # Receive results from children
            child_results = {}
            print("LOOOKING FOR CHILD RESULTS")
            print("current children product", node_info_local["children_product"]")
            for child_id in node_info_local["children_product"]:
                (sender_child, child_product) = comm.recv(source=child_id, tag = machine_id)
                child_results[child_id] = child_product
                print(f"COLLECTING CHILDRENNNN Worker {rank} - Received result from Child {child_id}: {child_product}")

            # Synchronize all processes before proceeding
            # Combine results from specific children (concatenate strings)
            #comm.Barrier() 
            print("child results",child_results)

            # Combine strings based on key order
            combined_result = "".join(child_results[key] for key in sorted(child_results.keys()))

            # Print the combined string
            print(combined_result)
        


            # Perform the current operation on the combined result
            current_product = calculate_string(combined_result, node_info_local["operations"][0], node_info_local["modulo"])
            print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][0]}, Result: {current_product}")
            
            # Send the result to the parent process
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            print("INTERMEDIATE NODE SENDING THIS", (machine_id, current_product))

            combined_string = "".join([child_results[child_id] for child_id in sorted(child_results.keys()) if child_id in node_info_local["children_product"]])
            print("this is machine", machine_id, "my children have sent me a result", combined_result)
            
    # Inform the master process that the worker has completed its tasks
    print(f"Worker {rank} - Completed all cycles. Sending completion signal to Master.")
    comm.send((machine_id, current_product), dest=MASTER)



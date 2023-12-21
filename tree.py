from mpi4py import MPI

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
wear_opname=["enhance","reverse","chop","trim","split"]
MASTER = 0

# Function to read and process the input file
def read_input_file(filename):
    with open(filename, 'r') as file:
        # Read the lines from the file and remove leading/trailing whitespaces
        lines = [line.strip() for line in file.readlines()]

    return lines

# Function to perform operations for each node
def calculate_string(product, operation, mod):
    print("calculate string this is the product",product,"this is the operation",operation,"this is the mod",mod)
    #NOTES
    #Take the threshold,calculate the weariness,do the current operation and update the index for next call
    #for leaf nodes do not add
    #for non leaf nodes add
    #each node has operations list and an index value for current operation
    return
    
# Master process
num_children = {}  
leaf_nodes = []              
if rank == MASTER:
    # Read and process the input file
    input_lines = read_input_file("input.txt")

    # Extract relevant information from input_lines
    num_machines = int(input_lines[0])
    num_cycles = int(input_lines[1])
    wear_factors = list(map(int, input_lines[2].split()))
    maintenance_threshold = int(input_lines[3])

    # Process child-parent relationships to identify leaf nodes
    child_parent_relationships = [list(map(str, line.split())) for line in input_lines[4:num_machines+3]]
    parent_set = set()

    # Initialize worker information for each leaf node
    node_info = {}
    num_children = {i: 0 for i in range(1, num_machines + 1)}
    for child, parent, operation_name in child_parent_relationships:
        parent=int(parent)
        child=int(child)
        parent_set.add(parent)
        operations=[]
        print("this is child",child,"this is parent",parent)
        if (child % 2 == 0) :
            operations =["enhance","split","chop"]
            mod=3 

        else :
            operations = ["trim","reverse"]
            mod=2

        node_info[child] = {
            "machine_id": child,
            "parent_id": parent,
            "initial_operation": operation_name,
            "operations": operations,
            "modulo": mod
            }
            # Add any other relevant information
        #find initial opration index in the operations 
        print(operations)
        current_op_index=operations.index(operation_name)
        #ADD NODE_INFO THE CURRENT OPERATION INDEX
        node_info[child]["current_op_number"]=current_op_index
            
        num_children[parent] += 1 #index i holds the number of children of node i

    # Determine leaf nodes (machines without parents)
    leaf_nodes = sorted(set(range(2, num_machines + 1)) - parent_set)
    print("leaf nodes",leaf_nodes)

    # Extract initial product names
    num_leaf_machines = len(leaf_nodes)
    products = input_lines[num_machines+3:num_machines+3+num_leaf_machines] # Assuming line number is the same as num_leaf_machines
    print("products",products)

    # Distribute necessary information to worker processes
    i=1
    for leaf_id, product in zip(leaf_nodes, products):
        # Send worker_info and product to the corresponding worker process
        print("sending to",leaf_id,"size i.e process number is",size,"this is i",i)
        comm.send((node_info[leaf_id], product,node_info,wear_factors), dest=i) #node info gönderdim haberlesme boyle cok yer kaplıyo sanırım ama baska türlü nasıl olucak ??????????
        i+=1


    # Receive the final result from the root node (ID 1)
    #final_result, _ = comm.recv(source=1)
    #print("Final Result:", final_result)

# Worker processes
else:
    print("else block worker")
    print("leaf nodes",leaf_nodes)
    # Receive information from the master process
    i=rank
    node_info, product,node_list,wear_factors = comm.recv(source=MASTER) #from master process to worker process
    print("node info",node_info,"product",product,"leaf child is ",node_info["machine_id"])
    result = f"Result from machine {node_info['machine_id']}"
    #for real result perform the current operation without adding,leaf nodes do not add 
    operation_performed=node_info["operations"][node_info["current_op_number"]] #current operation name
    mod= node_info["modulo"]
    #wear_opname=["enhance","reverse","chop","trim","split"] made this global 
    wear_fac_index=wear_opname.index(operation_performed)
    wear_amount=wear_factors[wear_fac_index]
    result=calculate_string(product,operation_performed,mod)

    # Send the result back to the parent node, if not the root node

    if node_info["machine_id"] != 1:
        parent_id=node_info["parent_id"]
        comm.send((parent_id,node_list,result), dest=i)   #nsend the result to parent,it will also calculate its own result and will send it to its own parent until it reaches the root node


    parent_id,node_list,result= comm.recv(source=i)
    parent_info=node_list[parent_id]
    product=result
    mod=parent_info["modulo"]
    operation_performed=parent_info["operations"][parent_info["current_op_number"]]
    wear_fac_index=wear_opname.index(operation_performed) #find the index of the operation name in the wear factors list
    wear_amount=wear_factors[wear_fac_index]

    result=calculate_string(product,operation_performed,mod) #calculate the result arrange the index 

    #create a for loop for the upper part that sends messages to the parent until the root parent is reached


    print("Cnode info taken from child my id is ",parent_info["machine_id"],"my parent is",parent_info["parent_id"],"initial operation is",parent_info["initial_operation"])
   
      
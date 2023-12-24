import sys
from mpi4py import MPI
import struct

#Basak Tepe 2020400117
#Irem Nur Yildirim 2020401042
#Group Number 38

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
cycle = 1
wear_opname = ["enhance", "reverse", "chop", "trim", "split"] # 4 3 1 2 3 
MASTER = 0
MAINTENANCE_TAG=555


#main

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) != 3:
        print("Usage: mpiexec -n 1 python yourcode.py input.txt output.txt")
        sys.exit(1)

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

# Input processing

def read_input_file(filename):
    with open(filename, 'r') as file:
        lines = [line.strip() for line in file.readlines()]

    return lines


# Function that performs string operations. It is also responsible for sending maintenance records.

def calculate_string(product, operation, mod,cycle_step,machine_id,accumulated_wear):

    #special node is node 1
    if operation == "special":
        #We send a dummy message to node 1
        data = (machine_id, 0, cycle_step) #so that root does not wait
        packed_data = struct.pack('iii', *data)
        comm.Isend(packed_data, dest=MASTER, tag=22222)
        return product,0

    #string operations
    if operation == "enhance":
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
    else:
        print("Invalid operation")
        # Send maintenance cost to main control room using non-blocking communication
    
    #Calculating wear-out
    wear_factor_index=wear_opname.index(operation) 
    wear_factor=wear_factors[wear_factor_index]

    # Take the threshold, calculate the weariness, do the current operation and update the index for the next call
    # if the threshold is exceeded, send the result to the parent node

    # Assuming machine_id, cost, and cycle_step are integers
    data = (machine_id, wear_factor, cycle_step) 
    #pack the data for isend
    packed_data = struct.pack('iii', *data)
    # Use Isend with the packed data
    req_send = comm.Isend(packed_data, dest=MASTER, tag=22222)
    return product,wear_factor


# Read and process the input file
input_lines = read_input_file(input_filename) 
# Extract relevant information from input_lines
num_machines = int(input_lines[0])
num_cycles = int(input_lines[1])
wear_factors = list(map(int, input_lines[2].split()))
maintenance_threshold = int(input_lines[3])

# Process child-parent relationships to identify leaf nodes
child_parent_operations = [list(map(str, line.split())) for line in input_lines[4:num_machines + 3]]

# Initialize worker information for each leaf node
node_info = {}
child_dict_of_root={}
num_children = {i: 0 for i in range(1, num_machines + 1)}
parent_set = set()
for child, parent, operation_name in child_parent_operations:
    parent = int(parent)
    child = int(child)
    parent_set.add(parent)
    #operation name is the first opertion that the machine will initially perform
    operations = []
    if (child % 2 == 0) :
            operations =["enhance","split","chop"]
            mod=3 

    else :
            operations = ["trim","reverse"]
            mod=2
    current_op_index=operations.index(operation_name)
    
    node_info[child] = {
        "machine_id": child,
        "parent_id": parent,
        "initial_operation": operation_name,
        "operations": operations,
        "modulo": mod,
        "children_product": {} , # dictionary of children id and their results
        "accumulated_wear": 0
    }
    node_info[child]["current_op_number"] = current_op_index
    if parent == 1:
        child_dict_of_root[child]=1
        node_info[parent] = {
            "machine_id": 1,
            "parent_id": 0,
            "initial_operation": "special",
            "operations": ["special"],
            "modulo": 1,
            "children_product": child_dict_of_root ,  # dictionary of children id and their results
            "current_op_number": 0, 
            "accumulated_wear": 0
        }

    num_children[parent] += 1 
    
    #we pass children node id information to parents
    if (int(parent)!= 1):
        node_info[parent]["children_product"][child] = 1 


# Determine leaf nodes (machines without parents)
leaf_nodes = sorted(set(range(2, num_machines + 1)) - parent_set)
#print("leaf nodes", leaf_nodes)

# Extract initial product names
num_leaf_machines = len(leaf_nodes)
products = input_lines[num_machines + 3:num_machines + 3 + num_leaf_machines]  # Assuming line number is the same as num_leaf_machines
#print("products", products)

file_name = output_filename
#empty the file by opening it with w
with open(file_name, 'w') as file:
    pass


wearout_logs = []
accumulated_wear_list = [0] * (num_machines + 1) # Index 0 not used


#Master process - Main Control Room
if rank == MASTER:

    for cycle_step in range(num_cycles):

        # Distribute necessary information to worker processes
        for i in range(1, size):
            if i <= len(leaf_nodes):
                machine_id = leaf_nodes[i - 1]
                initial_product = products[i - 1]
                #print(f"Sending initial information to worker {leaf_nodes[i - 1]} - Machine ID: {machine_id}, node info: {node_info[machine_id]}")
                comm.send((machine_id, initial_product, node_info[machine_id]), dest=leaf_nodes[i - 1])

        # Identify and distribute information to the remaining non-leaf nodes
        for node_id, node_data in node_info.items():
            if node_id not in leaf_nodes:
                machine_id = node_id
                initial_product = None
                #print(f"Sending initial information to worker for non-leaf node {node_id} - Machine ID: {machine_id}, node info: {node_data}")
                comm.send((machine_id, initial_product, node_data), dest=node_id)

        # Collect results from 1
        final_machine_id, final_result = comm.recv(source=1,tag=1)
        #print(f"Received result from worker {1} - Machine ID: {final_machine_id}, Result: {final_result}")

        #log the final result from root node (node 1)
        with open(file_name, 'a') as file:
            # Append the content of final_result to the file
            file.write(final_result + "\n")

            if (cycle_step == num_cycles-1): #we reached the final cycle. All final products are logged and we can also log wear-outs now.
                for item in wearout_logs:
                    file.write(f"{item}\n") 

        # Check for the special "lets continue looping" message from any worker
        for i in range(1, num_machines):
            # Receive message with tag 999999. This tag is sent by each worker at the end of each cycle to ensure synchronized looping.
            msg = comm.recv(source=i, tag=999999, status=MPI.Status())   

        #For loop for recieving maintenance logs
        for i in range(1, num_machines+1):
            status = MPI.Status()
            buf = bytearray(3 * MPI.INT.Get_size()) 
            # Non-blocking receive
            comm.Irecv(buf,source=i, tag=22222)
            received_data = struct.unpack('iii', buf)
            machine_id, cost, cycle_step_received = received_data


            if(machine_id <= num_machines):
                #we will check for wears that exceed the threshold
                accumulated_wear_list[machine_id] += cost
                #print("accumulated wear for machine id", machine_id, "is", accumulated_wear_list[machine_id])
                if accumulated_wear_list[machine_id] >= maintenance_threshold:
                    #print("maintenance is needed for machine id", machine_id,"because it is wearout is",accumulated_wear_list[machine_id],"this is cycle",cycle_step_received,"cost is",cost)
                    #print("cycle in master is :",cycle,"cycle step received is",cycle_step_received)
                    machine_cost=(accumulated_wear_list[machine_id] - maintenance_threshold + 1) * cost
                    #kebab case log string
                    kebab_case_report = f"{machine_id}-{machine_cost}-{cycle_step_received}"
                    #Store the log in a list of logs
                    wearout_logs.append(kebab_case_report)
                    #reset the wear-out as if we took care of it
                    accumulated_wear_list[machine_id] = 0
                    #if we are in the last cycle, log the maintenance records to the file
                    if cycle_step_received==num_cycles: 
                        with open(file_name, 'a') as file:
                            # Append the content of final_result to the file
                            file.write(f"{kebab_case_report}\n")
            else:
                print("machine id is not valid", machine_id)

        #if this is the last cycle
        if(cycle_step == num_cycles -1):
            #get ready to end the program
            sys.exit()

# Worker processes
else:
    for cycle_step in range(num_cycles):    
        # Receive identity and asset information from master process
        machine_id, initial_product, node_info_local = comm.recv(source=MASTER)
        #print(f"Worker {rank} - Received initial information - Machine ID: {machine_id}, local worker info: {node_info_local}")


        #Leaf process is sent an initial product from the master. It is not none.
        
        #Leaf nodes 
        if initial_product is not None:
            # Perform the current operation on the combined result

            #initial operation is
            initial_operation_name = node_info_local["initial_operation"]
            #operation list is
            operations_list = node_info_local["operations"]
            #mode is
            mod = len(operations_list)
            #get the initial index 
            index_of_initial_operation = operations_list.index(initial_operation_name)

            #calculate current operation index
            new_op_index= (index_of_initial_operation + cycle_step)%mod

            #operate on the string directly. 
            #recieve output product and wear offset
            current_product,wear_offset = calculate_string(initial_product, node_info_local["operations"][new_op_index], node_info_local["modulo"],cycle_step+1,machine_id,node_info_local["accumulated_wear"])
            #print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][new_op_index]}, Result: {current_product}")
            
            node_info_local["current_op_number"] = (node_info_local["current_op_number"] + 1) % node_info_local["modulo"] # Update operation index
            
            # Send the result to the parent process
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            #print("Leaf sent this", (machine_id, current_product))


        #Else you are not a leaf node and you dont have initial product. 
        #Collect products from your children.
            
        #Intermediary nodes
        else:
            # Receive results from children
            child_results = {}
            #print("I am collecting my children's results",cycle, "machine id is ", machine_id,"node info local", node_info_local)

            for child_id in node_info_local["children_product"]:
                (sender_child, child_product) = comm.recv(source=child_id, tag = machine_id)
                child_results[child_id] = child_product
                #print(f"Collecting children - Worker {rank} - Received result from Child {child_id}: {child_product}")

            #print("child results",child_results)

            # Combine strings based on key order
            combined_result = "".join(child_results[key] for key in sorted(child_results.keys()))
            #print("this is combined_result",combined_result)
        

            # Perform the current operation on the combined result
            #initial operation is
            initial_operation_name = node_info_local["initial_operation"]
            #operation list is
            operations_list = node_info_local["operations"]
            #mode is
            mod = len(operations_list)
            #get the initial index 
            index_of_initial_operation = operations_list.index(initial_operation_name)
            #calculate current operation index
            new_op_index= (index_of_initial_operation + cycle_step)%mod

            #operate on the string.
            #recieve output product and wear offset
            current_product,wear_offset = calculate_string(combined_result, node_info_local["operations"][new_op_index],mod,cycle_step+1,machine_id,node_info_local["accumulated_wear"])
            #print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][new_op_index]}, Result: {current_product}")
            
            # Send the result to the parent process             
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            #print("intermediate node sent", (machine_id, current_product))

            #you are the last/root node. send your result to master
            if machine_id == 1:
                comm.send((machine_id, combined_result), dest= MASTER, tag = 1)

        #A special continue loop message at the end of each loop 
        #with tag 999999 to synchronize loops of master and the loops we have in workers.
        comm.send("hihi",dest= MASTER, tag = 999999)    


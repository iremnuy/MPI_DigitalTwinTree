import sys
from mpi4py import MPI
import struct

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
cycle = 1
wear_opname = ["enhance", "reverse", "chop", "trim", "split"] # 4 3 1 2 3 
MASTER = 0
MAINTENANCE_TAG=555

# Function to read and process the input file
def read_input_file(filename):
    with open(filename, 'r') as file:
        # Read the lines from the file and remove leading/trailing whitespaces
        lines = [line.strip() for line in file.readlines()]

    return lines


# Function to perform operations for each node
def calculate_string(product, operation, mod,cycle_step,machine_id,accumulated_wear):
    print("calculate string this is the product", product, "this is the operation", operation, "this is the mod", mod)
    if operation == "special":
        print("special operation for root node")
        #send dummy message for acc 
        data = (machine_id, 0, cycle_step) #root için beklemesin diye 
        packed_data = struct.pack('iii', *data)
        print("\n &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&6 CS SENDS THE MACHINE ID", machine_id)
        comm.Isend(packed_data, dest=MASTER, tag=22222)
        return product,0
    # NOTES
    wear_factor_index=wear_opname.index(operation) 
    wear_factor=wear_factors[wear_factor_index]
    print("wear factor is ",wear_factor,"operation is ",operation)
    # Take the threshold, calculate the weariness, do the current operation and update the index for the next call
    # if the threshold is exceeded, send the result to the parent node
    if operation == "enhance":
        # duplicate the first and last letters
        product = product[0] + product + product[-1]
        #wear_factor_index=wear_opname.index(operation) 
        #wear_factor=wear_factors[wear_factor_index]
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
        # Send maintenance cost to main control room using non-blocking communication
    
    # Assuming machine_id, cost, and cycle_step are integers
    data = (machine_id, wear_factor, cycle_step) #buraya zaten girmeyecek hep tek seferlik wear factor geri gönderiypor mastera

            # Pack the data into a bytes-like object using struct.pack
    packed_data = struct.pack('iii', *data)

    print("\n &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&6 CS SENDS THE MACHINE ID", machine_id)
    # Use Isend with the packed data
    req_send = comm.Isend(packed_data, dest=MASTER, tag=22222)
    

    print("OPERATION RESULT ", product,"this is wear fac",wear_factor)
    return product,wear_factor

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) != 3:
        print("Usage: mpiexec -n 1 python yourcode.py input.txt output.txt")
        sys.exit(1)

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

# Read and process the input file
input_lines = read_input_file(input_filename) #instead take argumnts from command line
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
    node_info[child]["current_op_number"] = current_op_index #örneğin 4 numaralı child ın ilk op chop ise 2 olacak sonra number 1 arttırılıp 3 e göre modu alınacak 
    if parent == 1:
        child_dict_of_root[child]=1
        node_info[parent] = {
            "machine_id": 1,
            "parent_id": 0,
            "initial_operation": "special",
            "operations": ["special"],
            "modulo": 1,
            "children_product": child_dict_of_root ,  # dictionary of children id and their results
            "current_op_number": 0, #önemsiz kullanılmıyor ama hata veriyor o yüzden
            "accumulated_wear": 0
        }
    # find initial operation index in the operations
    # Index for the first operation in the list
    

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

file_name = output_filename
#empty the file
# Open the file in write mode ('w') to empty it
with open(file_name, 'w'):
    pass  # This block will just truncate the file

#tag for a wearout log is 11111 
wearout_logs = []
accumulated_wear_list = [0] * (num_machines + 1) # Index 0 is not used

if rank == MASTER:

    for cycle_step in range(num_cycles):

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

        # Collect results from 1
        final_machine_id, final_result = comm.recv(source=1,tag=1)
        print(f"Received result from worker {1} - Machine ID: {final_machine_id}, Result: {final_result}")
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Final Result:", final_result)
        # Specify the file name
        # Open the file in append mode ('a')
        with open(file_name, 'a') as file:
            # Append the content of final_result to the file
            file.write(final_result + "\n")  # Add a newline if needed
            if (cycle_step == num_cycles-1): #we are at the last stpe therefore we can log our wearouts//num_cycles-1 idi ona bir ekledim 
                for item in wearout_logs:
                    file.write(f"{item}\n") #son cycleda buraya tekrar dönmüyor oyüzden 2 4 10 yazılmıyo 

        cycle+=1
        # Check for the special continue loop message from any worker
        for i in range(1, num_machines):
            # Receive message with tag 999999
            msg = comm.recv(source=i, tag=999999, status=MPI.Status())   

        for i in range(1, num_machines+1): #root da gönderiyor 
            status = MPI.Status()
            # Prepare a buffer to receive the data
            buf = bytearray(3 * MPI.INT.Get_size())  # Adjust the size based on your data type

            # Non-blocking receive
            #req_recv = comm.Irecv(buf, source=i, tag=22222)
            req_recv = comm.Irecv(buf,source=i, tag=22222)

            # Unpack the received data using struct.unpack (assuming 3 integers in this example)
            received_data = struct.unpack('iii', buf)
            # Unpack received data
            machine_id, cost, cycle_step_received = received_data
            print("\n  *************************** MACHINE ID ", machine_id, "list size", len(accumulated_wear_list), received_data)

            if(machine_id <= num_machines):
                accumulated_wear_list[machine_id] += cost
                print("accumulated wear for machine id", machine_id, "is", accumulated_wear_list[machine_id])
                if accumulated_wear_list[machine_id] >= maintenance_threshold:
                    print("maintenance is needed for machine id", machine_id,"because it is wearout is",accumulated_wear_list[machine_id],"this is cycle",cycle_step_received,"cost is",cost)
                    # Create kebab case string
                    #print("cycle in master is :",cycle,"cycle step received is",cycle_step_received)
                    machine_cost=(accumulated_wear_list[machine_id] - maintenance_threshold + 1) * cost
                    kebab_case_report = f"{machine_id}-{machine_cost}-{cycle_step_received}"
                    print("kebab case report is",kebab_case_report)
                    #Store the kebab case string in the list
                    wearout_logs.append(kebab_case_report)
                    accumulated_wear_list[machine_id] = 0
                    if cycle_step_received==num_cycles: 
                        with open(file_name, 'a') as file:
                            # Append the content of final_result to the file
                            file.write(f"{kebab_case_report}\n") #son cycleda buraya tekrar dönmüyor oyüzden 2 4 10 yazılmıyo 
            else:
                print("machine id is not valid", machine_id)

# Worker processes
else:
    for cycle_step in range(num_cycles):    
        # Receive information from master process
        machine_id, initial_product, node_info_local = comm.recv(source=MASTER)
        print(f"Worker {rank} - Received initial information - Machine ID: {machine_id}, local worker info: {node_info_local}")

        # Perform operations for the specified number of cycles

        # Only collect results from children if the initial product is None (not a leaf)
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
            # If initial product is not None, directly perform the operation and send the result to the parent bc we are leaf
            #op_index=node_info_local["current_op_number"]
            current_product,wear_offset = calculate_string(initial_product, node_info_local["operations"][new_op_index], node_info_local["modulo"],cycle_step+1,machine_id,node_info_local["accumulated_wear"])
            print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][new_op_index]}, Result: {current_product}")
            node_info_local["current_op_number"] = (node_info_local["current_op_number"] + 1) % node_info_local["modulo"] # Update operation index for 
            # Send the result to the parent process
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            print("LEAF IS SENDING THIS", (machine_id, current_product))
            #comm.Barrier()

        else:
            # Receive results from children
            child_results = {}
            print("LOOOKING FOR CHILD RESULTS cycle is ",cycle, "machine id is ", machine_id,"node info local", node_info_local)
            print("current children product", node_info_local["children_product"])
            for child_id in node_info_local["children_product"]:
                print("waiting for this child id ",child_id)
                (sender_child, child_product) = comm.recv(source=child_id, tag = machine_id) #1 buradan alamıyor 
                child_results[child_id] = child_product
                print(f"COLLECTING CHILDRENNNN Worker {rank} - Received result from Child {child_id}: {child_product}")

            # Synchronize all processes before proceeding
            # Combine results from specific children (concatenate strings)
            #comm.Barrier() 
            print("child results",child_results)

            # Combine strings based on key order
            combined_result = "".join(child_results[key] for key in sorted(child_results.keys()))

            # Print the combined string
            print("this is combined_result",combined_result)
        

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
            current_product,wear_offset = calculate_string(combined_result, node_info_local["operations"][new_op_index],mod,cycle_step+1,machine_id,node_info_local["accumulated_wear"])
            print(f"Worker {rank} - Cycle {cycle + 1} - Operation: {node_info_local['operations'][new_op_index]}, Result: {current_product}")
            print("acc wear after update",node_info_local["accumulated_wear"])
            #node_info_local["current_op_number"] = (node_info_local["current_op_number"] + 1) % node_info_local["modulo"] # Update operation index for 
            #neden bunu yoruma aldıık 
            #WEAROUT CALCULATION with tag 22222
            #cost = 0

            #YAPILACAKLAR#################################################
            #cost calculation if accumulated wear >= threshold

            #C = (A − T + 1) ∗ W F, A ≥ T



            ##############################################################


            # Assuming machine_id, cost, and cycle_step are integers
            #data = (machine_id, cost, cycle_step)

            # Pack the data into a bytes-like object using struct.pack
            #packed_data = struct.pack('iii', *data)

            # Use Isend with the packed data
            #req_send = comm.Isend(packed_data, dest=MASTER, tag=22222)

            
            ##############################################################
            
            # Send the result to the parent process             
            print("calculated string is being sent to parent with id: ", node_info_local["parent_id"])
            #dest=1 2.node için 
            comm.send((machine_id, current_product), dest=node_info_local["parent_id"], tag = node_info_local["parent_id"])
            print("INTERMEDIATE NODE SENDING THIS", (machine_id, current_product))

            combined_string = "".join([child_results[child_id] for child_id in sorted(child_results.keys()) if child_id in node_info_local["children_product"]])
            print("this is machine", machine_id, "my children have sent me a result", combined_result)
            #you are the last node. send it to master
            if machine_id == 1:
                comm.send((machine_id, combined_result), dest= MASTER, tag = 1)

        #special continue loop message
        comm.send("hihi",dest= MASTER, tag = 999999)    

        # Inform the master process that the worker has completed its tasks
        #print(f"Worker {rank} - COMPLETED all cycles. Sending completion signal to Master.This is the output of cycle {cycle}", current_product,"this is current machine :",machine_id,"this is my parent", node_info_local["parent_id"])
        #comm.send((machine_id, current_product), dest=MASTER,tag=1)

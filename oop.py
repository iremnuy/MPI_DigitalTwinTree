from mpi4py import MPI

class Node:
    def __init__(self, machine_id, parent_id, operation_name, mod):
        self.machine_id = machine_id
        self.parent_id = parent_id
        self.initial_operation = operation_name
        self.operations = self.get_operations()
        self.mod = mod
        self.children_product = {} #concatenate the results of the children products 

    def get_operations(self):
        if self.machine_id % 2 == 0:
            return ["enhance", "split", "chop"]
        else:
            return ["trim", "reverse"]
        
    def send_to_parent(self, result):
        if self.parent_id != 1:
            self.comm.send((self.parent_id, result, self.machine_id), dest=self.parent_id, tag=self.parent_id)

    def receive_from_children(self):
        status = MPI.Status()
        result_of_child, child_id = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        parent_id = status.Get_tag()
        return parent_id, child_id, result_of_child
    

class MPIProgram:
    def __init__(self):
        # MPI initialization
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.MASTER = 0
        self.wear_opname = ["enhance", "reverse", "chop", "trim", "split"]

    def read_input_file(self, filename):
        with open(filename, 'r') as file:
            lines = [line.strip() for line in file.readlines()]
        return lines

    def calculate_string(self, product, operation, mod):
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
        return product

    def master_process(self):
        num_children = {}  
        leaf_nodes = []              
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

    # Instead use OOP to store information about each node
    
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
        print(operations)
        current_op_index=operations.index(operation_name)
        #ADD NODE_INFO THE CURRENT OPERATION INDEX
        node_info[child]["current_op_number"]=current_op_index
            
        num_children[parent] += 1 #index i holds the number of children of node i    

        #create a node object 
        node_info[child] = Node(child, parent, operation_name, mod)
        #find initial opration index in the operations 
       
        #node_info[parent]["children_product"]=[]

    # Determine leaf nodes (machines without parents)
    leaf_nodes = sorted(set(range(2, num_machines + 1)) - parent_set) #2den itibaren numaralandırılmış olduğunu varsayıyorum
    print("leaf nodes",leaf_nodes)

    # Extract initial product names
    num_leaf_machines = len(leaf_nodes)
    products = input_lines[num_machines+3:num_machines+3+num_leaf_machines] # Assuming line number is the same as num_leaf_machines
    print("products",products)

    # Instead make each node send its information to the corresponding worker process
    i=1
    for leaf_id, product in zip(leaf_nodes, products):
        #create initial nodes to send to workers
        i+=1


    # Receive the final result from the root node (ID 1)
        
    final_result = self.comm.recv(source=1, tag=1)
    print("Final Result:", final_result)

    def worker_process(self):
        # (Same as your worker process code)
        pass

    def run(self):
        if self.rank == self.MASTER:
            self.master_process()
        else:
            self.worker_process()

if __name__ == "__main__":
    mpi_program = MPIProgram()
    mpi_program.run()

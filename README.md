# CMPE 300 P2 

Our work is a parallel programming project for CMPE 300 Analysis of Algorithms course. It is written in python with mpi4py library. 
It provides an implementation of a digital twin for a factory by making use of N parallel instances for N number of machines in the factory. 

# Installation and running the project (Windows)

Follow these steps to get our program up and running on your local machine:
    -pip install mpi4py
    -mpiexec -n 44 python main.py input.txt output.txt

For the flag -n, please give at least a number greater than the number of existing machines.
Our implementation treats each machine as a seperate parallel process.

To check outputs, you can run our text_checker
    -python text_checker.py output.txt test_output.txt


# Features

- **Master to Worker Communication:** M2W communication enables us to distribute data packages that contain a worker node's identity, assets and responsibilities.
- **Worker to Worker Communication:** W2W communication enables us to distribute intermediary products from a worker to its parent. It enables us to percolate up in the tree structure with our intermediary products.
- **Worker to Master Communication:** W2M communication enables us to send maintenance records from machines (worker processes) to the main control room (master process) to be stored and logged once we finish all cycles.

# Notes
-Our code calculates all final products after N cycles correctly and logs them to the output file correctly.
(For the two inputs supplied in the description and assignment).

-For the maintenance records part, we also log the correct results only in different order.
We used a simple python script to check if outputs are the same by using sets. That small script is also included in this zip.
It stripts the texts and splits them into lines for comparison. (Strip is used to get rid of extra newline at the end in our output.txt)

All communications are correctly done at correct cycles for a correct number of times. Thus outputs are identical.

# Authors

Irem Nur Yildirim    2020401042
Basak Tepe           2020400117

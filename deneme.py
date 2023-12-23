from mpi4py import MPI
import numpy as np

#WINDOWS
#https://www.microsoft.com/en-us/download/details.aspx?id=105289
#mpiexec

#You may also check:
#https://www.mpich.org/downloads/

#Ubuntu (I didn't check)
#sudo apt install openmpi-bin openmpi-dev openmpi-common openmpi-doc libopenmpi-dev
#mpicc --showme:version

#MacOS (I didn't check)
#brew install open-mpi
#mpicc --showme:version


#mpi4py documentation
#https://mpi4py.readthedocs.io/en/stable/

#MPI Tutorial
#https://mpitutorial.com/tutorials/





#Initilazing MPI environment
comm = MPI.COMM_WORLD


size = comm.Get_size()
rank = comm.Get_rank()



if rank == 0:
    i=0
    while i<3:
        data = "root to leaf {i}"
        comm.send(data, dest = 1, tag = 0)
        print(rank, data)
        i+=1

else:
    print("rank",rank)
    data = comm.recv(source = 0, tag = 1)
    print(rank, data)
    data = "leaf to parent"
    comm.send(data, dest = 2, tag = 0)
    print(rank, data)
    data = comm.recv(source = 1, tag = 0)
    print(rank, data)

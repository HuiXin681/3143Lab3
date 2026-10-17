#include <stdio.h>
#include <mpi.h>

typedef struct {
    int rank, x, y;


}node;


int main(int argc, char *argv[]) {

    MPI_Init(&argc, &argv);

    int rank, size;
    node currNode;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Automatically determine grid dimensions
    int dims[2] = {0, 0};

    MPI_Dims_create(size, 2, dims); //https://www.mpich.org/static/docs/v3.1/www3/MPI_Dims_create.html

    int N = dims[0];  // Rows
    int M = dims[1];  // Columns

    currNode.y = rank / M;
    currNode.x = rank % M;
    currNode.rank = rank;

    printf("Node %d: Row %d, Column %d\n",
           rank, currNode.y, currNode.x);

    MPI_Finalize();

    return 0;
}
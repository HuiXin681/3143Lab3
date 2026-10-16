#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <omp.h>
#include <mpi.h>

typedef struct {
    int rank, x, y;


}node;

int main (int argc, char *argv[]) {
    int world_rank, world_size, ndims = 2, reorder;
    int grid_rank, grid_size;
    int nrows, ncols, nthreads;
    int provided;
    MPI_Comm cart_comm;
    int dims[ndims], coord[ndims];

    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &word_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    if (argc == 3) {
        nrows = atoi(argv[1]);
        ncols = atoi(argv[2]);
        nthreads = atoi(argv[3]);
        dims[0] = nrows;
        dims[1] = ncols;
        if (nrows*ncols != size) {
            if (rank == 0) printf("ERROR: nrows*ncols = %d * %d = %d != %d\n",
                nrows, ncols, nrows*ncols, size);
            MPI_Finalize();
            return 1;
        }
    }
    else {
        if (rank == 0) fprintf(stderr, "Usage: %s m n k\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    int periodical[2] = {0, 0};     // Ensures no wrap around boundaries on the grid
    reorder = 1;                    // Allows MPI to reorder ranks to improve performance while not affecting topology
    MPI_Cart_create(MPI_COMM_WORLD, ndims, dims, periodical, reorder, &cart_comm);

    MPI_Comm_size(cart_comm, &grid_size);
    MPI_Comm_rank(cart_comm, &grid_rank); // Match the work ranks to the optimised grid ranks

    MPI_Cart_coords(cart_comm, grid_rank, 2, coord); // get coordinates with the grid ranking

    int* status_arr = (int*) malloc(nthreads * sizeof(int)); // create shared array
#pragma omp parallel num_threads(nthreads) shared(status_arr) {
    int thread_id = omp_get_thread_num();

}
}

#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <stdlib.h>

#define IN_USE 1
#define FREE 0

//Probability of EV arriving/departing  (value x 10) %
#define ARRIVAL_PROB 3
#define DEPARTURE_PROB 2

#define NUM_OF_CHARGERS 4

typedef struct {
    int rank, x, y;

    int numChargers; //K

    int *chargerStatus;

}node;


void charger_thread(int chargerID, int *chargerStatus, int nodeID){

    int prevState = 0;
    int randVal;
    //Only want this to run ever second
    while(1){
        
        sleep(1);

        randVal = rand() % 10;

        if(chargerStatus[chargerID] == FREE){
            if(randVal <= ARRIVAL_PROB){
                chargerStatus[chargerID] = IN_USE;
                printf("Charger %d status: %d\n", chargerID, chargerStatus[chargerID]);
            }
        }
        else {
            if(randVal <= DEPARTURE_PROB){
                chargerStatus[chargerID] = FREE;
                printf("Charger %d status: %d\n", chargerID + (nodeID * NUM_OF_CHARGERS), chargerStatus[chargerID]);
            }
        }


    }

    return;
}



int main(int argc, char *argv[]) {

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);

    if(provided < MPI_THREAD_FUNNELED){
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

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

    currNode.numChargers = NUM_OF_CHARGERS;           //Change to dynamic range later TODO
    currNode.chargerStatus = (int*)calloc(currNode.numChargers, sizeof(int));

    if(currNode.chargerStatus == NULL){
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    printf("Node %d: Row %d, Column %d\n",
           rank, currNode.y, currNode.x);

    //Start threads
    #pragma omp parallel num_threads(currNode.numChargers)
    {
        int chargerID = omp_get_thread_num();

        charger_thread(chargerID, currNode.chargerStatus, currNode.rank);
    }

    free(currNode.chargerStatus);

    MPI_Finalize();

    return 0;
}
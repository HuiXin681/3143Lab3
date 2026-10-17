#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <stdlib.h>
#include <time.h>

#define IN_USE 1
#define FREE 0

//Probability of EV arriving/departing  (value x 10) %
#define ARRIVAL_PROB 3
#define DEPARTURE_PROB 2

#define NUM_OF_CHARGERS 4
#define BOOKKEEPER 0
#define TIMESTAMP_FREQ 5    //Seconds

typedef struct {
    int rank, x, y;

    int numChargers; //K

    int *chargerStatus;

}node;

void node_bookkeeper(node *currNode){

    time_t currentTime;
    struct tm t;

    while(1){

        sleep(2);

        currentTime = time(NULL);
        localtime_r(&currentTime, &t);

        int year = t.tm_year + 1900;
        int month = t.tm_mon + 1;
        int day = t.tm_mday;
        int hour = t.tm_hour;
        int minute = t.tm_min;
        int second = t.tm_sec;

        int available = 0;

        for(int i = 0; i < currNode->numChargers; i++){

            if(currNode->chargerStatus[i] == FREE){
                available++;
            }

        }



        printf("|Node %d| %04d | %02d | %02d | %02d | %02d | %02d | Available = %d|\n",
            currNode->rank, year, month, day, hour, minute, second, available);


        printf("");

    }
}


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
                //printf("Charger %d status: %d\n", chargerID, chargerStatus[chargerID]);
            }
        }
        else {
            if(randVal <= DEPARTURE_PROB){
                chargerStatus[chargerID] = FREE;
                //printf("Charger %d status: %d\n", chargerID + (nodeID * NUM_OF_CHARGERS), chargerStatus[chargerID]);
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

    //Start threads, +1 for the main thread (bookkeeper)
    #pragma omp parallel num_threads(currNode.numChargers + 1)
    {
        int threadID = omp_get_thread_num();

        if(threadID == BOOKKEEPER){
            node_bookkeeper(&currNode);
        }
        else{
            charger_thread(threadID, currNode.chargerStatus, currNode.rank);
        }

    }

    free(currNode.chargerStatus);

    MPI_Finalize();

    return 0;
}
#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define IN_USE 1
#define FREE -1

//Probability of EV arriving/departing  (value x 10) %
#define ARRIVAL_PROB 7
#define DEPARTURE_PROB 3

#define NUM_OF_CHARGERS 4
#define BOOKKEEPER 0
#define TIMESTAMP_FREQ 5    //Seconds

#define AVAILABILITY_THRESHOLD 1
#define REQUEST_AVl 1       //A node requests availability
#define RESPONSE_TAG 2      //Response message tag

#define NUM_OF_NEIGHBOURS 4

typedef struct{
    int carID;
}car;

typedef struct {
    //(0,0) is top left
    int rank, x, y;

    int numChargers; //K

    int *chargerStatus;

    int neighbours[4];

}node;

int count_available(node *currNode){

    int available = 0;

    for(int i = 0; i < currNode->numChargers; i++){

        int status;

        //Ensures does read a variable that is partially written
        #pragma omp atomic read
        status = currNode->chargerStatus[i];

        if(status == FREE){
            available++;
        }

    }

    return available;
}

void prob_inbox(node *currNode){

    int flag;
    MPI_Status status;

    //Check for incoming messages without blocking
    MPI_Iprobe(MPI_ANY_SOURCE, REQUEST_AVl, MPI_COMM_WORLD, &flag, &status);

    while(flag){

        int request;
        MPI_Recv(&request, 1, MPI_INT, status.MPI_SOURCE, REQUEST_AVl, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        //Calcs the total available chargers
        int available = count_available(currNode);

        //Send answer back
        MPI_Send(&available, 1, MPI_INT, status.MPI_SOURCE, RESPONSE_TAG, MPI_COMM_WORLD);

        //More requests?
        MPI_Iprobe(MPI_ANY_SOURCE, REQUEST_AVl, MPI_COMM_WORLD, &flag, &status);
    }

}

void request_availability(node *currNode){

    int request = 1;
    int neighbourAvailability[NUM_OF_NEIGHBOURS];

    MPI_Request outRequests[NUM_OF_NEIGHBOURS];
    MPI_Request inRequests[NUM_OF_NEIGHBOURS];

    int numRequests = 0;

    for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){
        
        int neighbour = currNode->neighbours[i];
        
        if(neighbour == MPI_PROC_NULL){
            //No neighbour in that position
            continue;
        }

        MPI_Irecv(&neighbourAvailability[i], 1, MPI_INT, neighbour, RESPONSE_TAG, MPI_COMM_WORLD, &inRequests[numRequests]);

        MPI_Isend(&request, 1, MPI_INT, neighbour, REQUEST_AVl, MPI_COMM_WORLD, &outRequests[numRequests]);
        numRequests++;

    }
    int finished = 0;

    while(!finished){

        prob_inbox(currNode);

        MPI_Testall(numRequests, inRequests, &finished, MPI_STATUSES_IGNORE);

        if(!finished){
            usleep(10000);
        }
    }
    MPI_Waitall(numRequests, outRequests, MPI_STATUSES_IGNORE);
    int foundAvailable = 0;

    for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

        if(currNode->neighbours[i] == MPI_PROC_NULL){
            continue;
        }

        if(neighbourAvailability[i] >= AVAILABILITY_THRESHOLD){

            printf("Node %d: Redirect EV to node %d (%d free chargers)\n",
                   currNode->rank,
                   currNode->neighbours[i],
                   neighbourAvailability[i]);

            foundAvailable = 1;
            break;
        }
    }
    if(!foundAvailable){

        printf("CONGESTION: Node %d and all neighbouring nodes are congested\n",
               currNode->rank);

        //TODO: Send congestion report to base station
    }


}

void node_bookkeeper(node *currNode){

    time_t currentTime;
    struct tm t;

    double lastCheck = MPI_Wtime();
    int nextCarID = 0;
    while(1){

        prob_inbox(currNode);
        
        //Spawn car?
        int randVal = rand() % 10;

        if (randVal < ARRIVAL_PROB){
            //Spawn new car
            car newCar;
            newCar.carID = currNode->rank * 100000 + nextCarID;        //Unqiue assuming each node does contain 100 thousand cars
            nextCarID++;

            //Find available:
            int availableChargerID = NULL;
            for(int i = 0; i<NUM_OF_CHARGERS; i++){
                if(currNode->chargerStatus[i] == FREE){
                    availableChargerID = i;
                    break
                }
            }

            int available = count_available(currNode);

            if(availableChargerID != NULL && available < AVAILABILITY_THRESHOLD){
                //help
            }


        }

        if(MPI_Wtime() - lastCheck >= TIMESTAMP_FREQ){
            lastCheck = MPI_Wtime();
            
            currentTime = time(NULL);
            localtime_r(&currentTime, &t);

            int year = t.tm_year + 1900;
            int month = t.tm_mon + 1;
            int day = t.tm_mday;
            int hour = t.tm_hour;
            int minute = t.tm_min;
            int second = t.tm_sec;

            int available = count_available(currNode);

            printf("|Node %d| %04d | %02d | %02d | %02d | %02d | %02d | Available = %d|\n",
            currNode->rank, year, month, day, hour, minute, second, available);

            if(available < AVAILABILITY_THRESHOLD){

                printf("Node %d congested. Requesting neighbour availability...\n",
                       currNode->rank);

                request_availability(currNode);
            }

        }
        sleep(1);


    }
}


void charger_thread(int chargerID, int *chargerStatus, int nodeID){

    int randVal;
    int status;

    //Only want this to run every second
    while(1){

        sleep(1);

        randVal = rand() % 10;

        #pragma omp atomic read
        status = chargerStatus[chargerID];

        if(status == FREE){

            if(randVal < ARRIVAL_PROB){

                #pragma omp atomic write
                chargerStatus[chargerID] = IN_USE;

            }
        }
        else{

            if(randVal < DEPARTURE_PROB){

                #pragma omp atomic write
                chargerStatus[chargerID] = FREE;

            }
        }
    }
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

    //Populate Neighbour's
    //Neighbour Logic. Null if no neighbour on that side
    currNode.neighbours[0] = (currNode.y > 0) ? rank - M : MPI_PROC_NULL;
    currNode.neighbours[1] = (currNode.y < N - 1) ? rank + M : MPI_PROC_NULL;
    currNode.neighbours[2] = (currNode.x > 0) ? rank - 1 : MPI_PROC_NULL;
    currNode.neighbours[3] = (currNode.x < M - 1) ? rank + 1 : MPI_PROC_NULL;

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
            charger_thread(threadID -1, currNode.chargerStatus, currNode.rank);
        }

    }

    free(currNode.chargerStatus);

    MPI_Finalize();

    return 0;
}
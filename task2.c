#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define FREE -1

//Probability of EV arriving/departing (value x 10) %
#define ARRIVAL_PROB 9
#define DEPARTURE_PROB 3

#define NUM_OF_CHARGERS 4
#define BOOKKEEPER 0
#define TIMESTAMP_FREQ 5    //Seconds

#define AVAILABILITY_THRESHOLD 1

#define REQUEST_AVl 1       //A node requests availability
#define RESPONSE_TAG 2      //Availability response message
#define CAR_TRANSFER_TAG 3  //Transfer a car to another node
#define CAR_ACCEPT_TAG 4    //Accept/reject car transfer

#define NUM_OF_NEIGHBOURS 4
#define REPORT_TIMEOUT 10   //SEconds

#define SIMULATION_TIME 60  //Seconds

#define REPORT_TAG 5
#define BASE_RESPONSE_TAG 6
#define STOP_TAG 7
#define STOP_ACK_TAG 8
#define TERMINATE_TAG 9


typedef struct{
    int carID;
}car;


typedef struct {
    int nodeRank;
    int x;
    int y;
    int availableChargers;
    int neighbourRanks[4];
    int neighbourAvailability[4];
    double timestamp;
    car Car;
} report;


typedef struct {
    //(0,0) is top left
    int rank, x, y;

    int numChargers; //K

    //Stores car IDs, FREE (-1) if no car
    int *chargerStatus;

    //Neighbours: Up, Down, Left, Right
    int neighbours[NUM_OF_NEIGHBOURS];

}node;


typedef struct {
    int availableChargers;
    double timestamp;
    int known;
} nodeAvailability;


//Shared between bookkeeper and charger threads
int terminateSimulation = 0;
int stopRequested = 0;



int count_available(node *currNode){

    int available = 0;

    #pragma omp critical(chargers)
    {
        for(int i = 0; i < currNode->numChargers; i++){

            if(currNode->chargerStatus[i] == FREE){
                available++;
            }

        }
    }

    return available;
}



//Assign car to an available charger
int assign_car(node *currNode, car newCar){

    int charger = -1;

    #pragma omp critical(chargers)
    {
        for(int i = 0; i < currNode->numChargers; i++){

            if(currNode->chargerStatus[i] == FREE){

                currNode->chargerStatus[i] = newCar.carID;
                charger = i;
                break;

            }
        }
    }

    if(charger != -1){

        printf("Node %d: Car %d assigned to charger %d\n",
               currNode->rank, newCar.carID, charger);

        return 1;
    }

    return 0; //No charger available
}



//Check incoming MPI messages
void prob_inbox(node *currNode){

    int flag;
    MPI_Status status;

    //Check for incoming availability requests without blocking
    MPI_Iprobe(MPI_ANY_SOURCE, REQUEST_AVl, MPI_COMM_WORLD, &flag, &status);

    while(flag){

        int request;

        MPI_Recv(&request, 1, MPI_INT,
                 status.MPI_SOURCE, REQUEST_AVl,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //Calculate total available chargers
        int available = count_available(currNode);

        //Send answer back
        MPI_Send(&available, 1, MPI_INT,
                 status.MPI_SOURCE, RESPONSE_TAG,
                 MPI_COMM_WORLD);

        //More requests?
        MPI_Iprobe(MPI_ANY_SOURCE, REQUEST_AVl,
                   MPI_COMM_WORLD, &flag, &status);
    }


    //Check for incoming car transfers
    MPI_Iprobe(MPI_ANY_SOURCE, CAR_TRANSFER_TAG,
               MPI_COMM_WORLD, &flag, &status);

    while(flag){

        car receivedCar;

        //Receive car ID from neighbouring node
        MPI_Recv(&receivedCar.carID, 1, MPI_INT,
                 status.MPI_SOURCE, CAR_TRANSFER_TAG,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //Attempt to assign received car
        int accepted = assign_car(currNode, receivedCar);

        //Tell sending node whether car was accepted
        MPI_Send(&accepted, 1, MPI_INT,
                 status.MPI_SOURCE, CAR_ACCEPT_TAG,
                 MPI_COMM_WORLD);

        if(!accepted){

            printf("Node %d: Rejected Car %d (No free chargers)\n",
                   currNode->rank, receivedCar.carID);

        }

        //More car transfers?
        MPI_Iprobe(MPI_ANY_SOURCE, CAR_TRANSFER_TAG,
                   MPI_COMM_WORLD, &flag, &status);
    }


    //Check if the base station wants us to stop generating cars
    MPI_Iprobe(MPI_ANY_SOURCE, STOP_TAG,
               MPI_COMM_WORLD, &flag, &status);

    if(flag){

        int message;

        MPI_Recv(&message, 1, MPI_INT,
                 status.MPI_SOURCE, STOP_TAG,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        stopRequested = 1;
    }

}



//Request availability from all neighboouring nodes
void request_availability(node *currNode, int *neighbourAvailability){

    int request = 1;

    MPI_Request outRequests[NUM_OF_NEIGHBOURS];
    MPI_Request inRequests[NUM_OF_NEIGHBOURS];

    int numRequests = 0;

    for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

        int neighbour = currNode->neighbours[i];

        neighbourAvailability[i] = -1;

        if(neighbour == MPI_PROC_NULL){
            //No neighbour in that position
            continue;
        }

        //Prepare to receive neighbour availability
        MPI_Irecv(&neighbourAvailability[i], 1, MPI_INT,
                  neighbour, RESPONSE_TAG, MPI_COMM_WORLD,
                  &inRequests[numRequests]);

        //Send availability request
        MPI_Isend(&request, 1, MPI_INT,
                  neighbour, REQUEST_AVl, MPI_COMM_WORLD,
                  &outRequests[numRequests]);

        numRequests++;
    }

    int finished = 0;

    //Wait for responses while still handling incoming messages
    while(!finished){

        prob_inbox(currNode);

        MPI_Testall(numRequests, inRequests,
                    &finished, MPI_STATUSES_IGNORE);

        if(!finished){
            usleep(10000);
        }
    }

    MPI_Waitall(numRequests, outRequests, MPI_STATUSES_IGNORE);

}



report create_report(node *currNode, car *newCar, int *neighbourAvailability){

    report newReport;

    //Node shit
    newReport.nodeRank = currNode->rank;
    newReport.x = currNode->x;
    newReport.y = currNode->y;
    newReport.availableChargers = count_available(currNode);

    //Copy neighbouring node information
    for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

        newReport.neighbourRanks[i] = currNode->neighbours[i];
        newReport.neighbourAvailability[i] = neighbourAvailability[i];

    }

    //Timestamp
    newReport.timestamp = (double)time(NULL);

    //Car information
    newReport.Car = *newCar;

    return newReport;
}



//Transfer car to another MPI node
int transfer_car(node *currNode, car newCar, int targetNode){

    int accepted = 0;

    MPI_Request outRequest;
    MPI_Request inRequest;

    //Prepare to receive acceptance confirmation
    MPI_Irecv(&accepted, 1, MPI_INT,
              targetNode, CAR_ACCEPT_TAG,
              MPI_COMM_WORLD, &inRequest);

    //Send car ID to target node
    MPI_Isend(&newCar.carID, 1, MPI_INT,
              targetNode, CAR_TRANSFER_TAG,
              MPI_COMM_WORLD, &outRequest);

    int finished = 0;

    //Wait for response while handling incoming requests
    while(!finished){

        prob_inbox(currNode);

        //Check if non blocking operation hs finished
        MPI_Test(&inRequest, &finished, MPI_STATUS_IGNORE);

        if(!finished){
            usleep(10000);
        }
    }

    MPI_Wait(&outRequest, MPI_STATUS_IGNORE);

    if(accepted){

        printf("Node %d: Car %d transferred to Node %d\n",
               currNode->rank, newCar.carID, targetNode);

    }

    return accepted;
}



//Find neighbour with most available chargers and redirect car
int redirect_car(node *currNode, car newCar){

    int neighbourAvailability[NUM_OF_NEIGHBOURS];
    int originalAvailability[NUM_OF_NEIGHBOURS];

    //Request availability from neighbouring nodes
    request_availability(currNode, neighbourAvailability);

    //Keep original values for the congestion report
    for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){
        originalAvailability[i] = neighbourAvailability[i];
    }

    //Try neighbours in order of most available chargers
    for(int attempt = 0; attempt < NUM_OF_NEIGHBOURS; attempt++){

        int bestNeighbour = -1;
        int bestIndex = -1;
        int mostAvailable = 0;

        for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

            if(currNode->neighbours[i] == MPI_PROC_NULL){
                continue;
            }

            if(neighbourAvailability[i] > mostAvailable){

                mostAvailable = neighbourAvailability[i];
                bestNeighbour = currNode->neighbours[i];
                bestIndex = i;
            }
        }

        //No neighbours with available chargers
        if(bestNeighbour == -1){
            break;
        }

        printf("Node %d: Attempting to redirect Car %d to Node %d (%d free chargers)\n",
               currNode->rank, newCar.carID,
               bestNeighbour, mostAvailable);

        //Try transferring car
        if(transfer_car(currNode, newCar, bestNeighbour)){
            return 1;
        }

        //Neighbour rejected car, try next best neighbour
        neighbourAvailability[bestIndex] = -1;
    }

    printf("CONGESTION: Node %d could not redirect Car %d\n",
           currNode->rank, newCar.carID);


    //Send congestion report to base station
    report congestionReport = create_report(currNode, &newCar, originalAvailability);

    //Get base station rank
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int baseStationRank = size - 1;

    //Send congestion report
    MPI_Send(&congestionReport, sizeof(report), MPI_BYTE,
             baseStationRank, REPORT_TAG, MPI_COMM_WORLD);


    //Receive recommended nodes from base station
    int recommendations[size];

    MPI_Recv(recommendations, size, MPI_INT,
             baseStationRank, BASE_RESPONSE_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    int numRecommendations = recommendations[0];

    if(numRecommendations == 0){

        printf("Node %d: Base station found no nearby available nodes\n",
               currNode->rank);

        return 0;
    }

    printf("Node %d: Base station suggested %d nodes\n",
           currNode->rank, numRecommendations);

    //Try recommended nodes
    for(int i = 0; i < numRecommendations; i++){

        int targetNode = recommendations[i + 1];

        printf("Node %d: Trying base station recommendation Node %d\n",
               currNode->rank, targetNode);

        if(transfer_car(currNode, newCar, targetNode)){
            return 1;
        }
    }

    printf("Node %d: No recommended nodes accepted Car %d\n",
           currNode->rank, newCar.carID);

    return 0;
}



void node_bookkeeper(node *currNode){

    //Bookkeeper thread

    time_t currentTime;
    struct tm t;

    double lastCheck = MPI_Wtime();
    double lastSpawn = MPI_Wtime();

    int nextCarID = 0;

    //Random seed for this node
    srand((unsigned int)time(NULL) + currNode->rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int baseStationRank = size - 1;

    int acknowledged = 0;

    while(1){

        //Check incoming MPI messages
        prob_inbox(currNode);


        //Stop generating cars but continue handling messages
        if(stopRequested){

            if(!acknowledged){

                int message = 1;

                MPI_Send(&message, 1, MPI_INT,
                         baseStationRank, STOP_ACK_TAG,
                         MPI_COMM_WORLD);

                acknowledged = 1;
            }

            int flag;
            MPI_Status status;

            MPI_Iprobe(baseStationRank, TERMINATE_TAG,
                       MPI_COMM_WORLD, &flag, &status);

            if(flag){

                int message;

                MPI_Recv(&message, 1, MPI_INT,
                         baseStationRank, TERMINATE_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                #pragma omp atomic write
                terminateSimulation = 1;

                break;
            }

            usleep(10000);
            continue;
        }


        //Chance of spawning a car every second
        if(MPI_Wtime() - lastSpawn >= 1){

            lastSpawn = MPI_Wtime();

            int randVal = rand() % 10;

            if(randVal < ARRIVAL_PROB){

                //Spawn new car
                car newCar;

                //Unique car ID (assuming less than 100000 cars per node) HAHA
                newCar.carID = currNode->rank * 100000 + nextCarID;
                nextCarID++;

                printf("Node %d: Car %d spawned\n",
                       currNode->rank, newCar.carID);

                //Check local availability
                int available = count_available(currNode);

                if(available >= AVAILABILITY_THRESHOLD){

                    //Assign car to local charger
                    if(!assign_car(currNode, newCar)){

                        //Local assignment failed, try neighbours
                        redirect_car(currNode, newCar);
                    }

                }
                else{

                    //No local availability, request neighbour data
                    printf("Node %d: Not enough chargers available. Checking neighbours...\n",
                           currNode->rank);

                    redirect_car(currNode, newCar);

                }
            }
        }


        //Bookkeeping timestamp every TIMESTAMP_FREQ seconds
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
                   currNode->rank, year, month, day,
                   hour, minute, second, available);

        }

        //Short sleep
        usleep(10000);
    }
}



//Charger thread - handles cars finishing charging
void charger_thread(int chargerID, int *chargerStatus, int nodeID){

    int randVal;

    //Each charger has its own random seed
    unsigned int seed = (unsigned int)time(NULL)
                      ^ ((unsigned int)(nodeID + 1) * 2654435761u)
                      ^ (unsigned int)(chargerID + 1);

    //Only want this to run every second
    while(1){

        sleep(1);

        int finished = 0;

        #pragma omp atomic read
        finished = terminateSimulation;

        if(finished){
            break;
        }

        randVal = rand_r(&seed) % 10;

        int departedCar = FREE;

        #pragma omp critical(chargers)
        {
            //If a car is currently charging
            if(chargerStatus[chargerID] != FREE){

                //Chance of car finishing charging
                if(randVal < DEPARTURE_PROB){

                    departedCar = chargerStatus[chargerID];
                    chargerStatus[chargerID] = FREE;

                }
            }
        }

        if(departedCar != FREE){

            printf("Node %d: Car %d finished charging at charger %d\n",
                   nodeID, departedCar, chargerID);

        }
    }
}



int availability_stale_check(int nodeRank, nodeAvailability *availabilityHistory){

    if(!availabilityHistory[nodeRank].known){
        return 0;
    }

    double currentTime = (double)time(NULL);

    if(currentTime - availabilityHistory[nodeRank].timestamp > REPORT_TIMEOUT){
        return 0;
    }

    return 1;
}



//Calculate distance between two nodes
int node_distance(int x1, int y1, int x2, int y2){

    return abs(x1 - x2) + abs(y1 - y2);
}



//Process reports and recommend available nodes
void baseStation_process(){

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int numNodes = size - 1;

    //Store the most recent report from each node
    report *reportHistory = calloc(numNodes, sizeof(report));

    //Store the most recent known availability for each node
    nodeAvailability *availabilityHistory = calloc(numNodes, sizeof(nodeAvailability));

    //Track which nodes have sent their own reports
    int *reportReceived = calloc(numNodes, sizeof(int));

    if(reportHistory == NULL || availabilityHistory == NULL ||
       reportReceived == NULL){

        MPI_Abort(MPI_COMM_WORLD, 1);
    }


    //Automatically determine grid dimensions
    int dims[2] = {0, 0};
    MPI_Dims_create(numNodes, 2, dims);

    int N = dims[0];
    int M = dims[1];


    //Output file for simulation results
    FILE *logFile = fopen("base_station_log.csv", "w");

    if(logFile == NULL){

        printf("Base station: Could not open log file\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    fprintf(logFile,
            "Time,Event,Sender,CarID,AvailableChargers,Alerts,Messages,RecommendedNodes\n");

    fflush(logFile);


    int totalAlerts = 0;
    int totalMessages = 0;
    int totalEvents = 0;

    int stopping = 0;
    int acknowledgements = 0;

    int *acknowledged = calloc(numNodes, sizeof(int));

    if(acknowledged == NULL){
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    double startTime = MPI_Wtime();

    printf("Base station started\n");
    fflush(stdout);     //Was having issues with print satements this seems to fix it


    while(1){

        //Check if the simulation time has finished
        if(!stopping && MPI_Wtime() - startTime >= SIMULATION_TIME){

            stopping = 1;

            printf("Base station: Simulation time finished\n");

            //Tell all nodes to stop generating new cars
            for(int i = 0; i < numNodes; i++){

                int message = 1;

                MPI_Send(&message, 1, MPI_INT,
                         i, STOP_TAG, MPI_COMM_WORLD);

                totalMessages++;
            }

            fprintf(logFile, "%.2f,STOP,-1,-1,-1,%d,%d,\n",
                    MPI_Wtime() - startTime,
                    totalAlerts, totalMessages);

            fflush(logFile);
        }


        //Check incoming messages without blocking
        int flag;
        MPI_Status status;

        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG,
                   MPI_COMM_WORLD, &flag, &status);

        if(flag){

            int nodeRank = status.MPI_SOURCE;


            //Receive acknowledgement from charging node
            if(status.MPI_TAG == STOP_ACK_TAG){

                int message;

                MPI_Recv(&message, 1, MPI_INT,
                         nodeRank, STOP_ACK_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                totalMessages++;
                totalEvents++;

                if(!acknowledged[nodeRank]){

                    acknowledged[nodeRank] = 1;
                    acknowledgements++;
                }

                printf("Base station: Node %d stopped generating cars\n",
                       nodeRank);

                fprintf(logFile, "%.2f,STOP_ACK,%d,-1,-1,%d,%d,\n",
                        MPI_Wtime() - startTime,
                        nodeRank, totalAlerts, totalMessages);

                fflush(logFile);
            }


            //Receive congestion report
            else if(status.MPI_TAG == REPORT_TAG){

                report receivedReport;

                MPI_Recv(&receivedReport, sizeof(report), MPI_BYTE,
                         nodeRank, REPORT_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                totalMessages++;
                totalEvents++;
                totalAlerts++;

                if(nodeRank < 0 || nodeRank >= numNodes){
                    continue;
                }


                //Save latest report from this node
                reportHistory[nodeRank] = receivedReport;
                reportReceived[nodeRank] = 1;


                //Mark that we have received a report
                if(!availabilityHistory[nodeRank].known ||
                   receivedReport.timestamp >= availabilityHistory[nodeRank].timestamp){

                    availabilityHistory[nodeRank].availableChargers =
                        receivedReport.availableChargers;

                    availabilityHistory[nodeRank].timestamp =
                        receivedReport.timestamp;

                    availabilityHistory[nodeRank].known = 1;
                }


                //Update neighbouring node availability from the report
                for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

                    int neighbourRank = receivedReport.neighbourRanks[i];

                    if(neighbourRank >= 0 && neighbourRank < numNodes){

                        if(receivedReport.neighbourAvailability[i] >= 0){

                            //Only update if the information is newer
                            if(!availabilityHistory[neighbourRank].known ||
                               receivedReport.timestamp >= availabilityHistory[neighbourRank].timestamp){

                                availabilityHistory[neighbourRank].availableChargers =
                                    receivedReport.neighbourAvailability[i];

                                availabilityHistory[neighbourRank].timestamp =
                                    receivedReport.timestamp;

                                availabilityHistory[neighbourRank].known = 1;
                            }
                        }
                    }
                }


                printf("Base station: Received report from Node %d\n",
                       nodeRank);


                //Check whether all direct neighbours have sent recent reports
                int allNeighboursReported = 1;

                for(int i = 0; i < NUM_OF_NEIGHBOURS; i++){

                    int neighbourRank = receivedReport.neighbourRanks[i];

                    if(neighbourRank < 0 || neighbourRank >= numNodes){
                        continue;
                    }

                    if(!reportReceived[neighbourRank]){

                        allNeighboursReported = 0;
                        break;
                    }

                    //Check whether the neighbour's own report is recent
                    double currentTime = (double)time(NULL);

                    if(currentTime - reportHistory[neighbourRank].timestamp > REPORT_TIMEOUT){

                        allNeighboursReported = 0;
                        break;
                    }
                }


                //Array storing the suggested nodes
                //First value is the number of recommendations
                int recommendations[size];

                for(int i = 0; i < size; i++){
                    recommendations[i] = -1;
                }

                recommendations[0] = 0;

                int shortestDistance = -1;


                //Only suggest alternatives when direct neighbour information is missing
                if(!allNeighboursReported){

                    //Search all known available nodes
                    for(int i = 0; i < numNodes; i++){

                        if(i == nodeRank){
                            continue;
                        }

                        //Only consider recent availability information
                        if(!availability_stale_check(i, availabilityHistory)){
                            continue;
                        }

                        if(availabilityHistory[i].availableChargers < AVAILABILITY_THRESHOLD){
                            continue;
                        }

                        int x = i % M;
                        int y = i / M;

                        int distance = node_distance(
                            receivedReport.x,
                            receivedReport.y,
                            x, y
                        );

                        //Direct neighbours have already been checked
                        if(distance <= 1){
                            continue;
                        }

                        //Found a closer available node
                        if(shortestDistance == -1 || distance < shortestDistance){

                            shortestDistance = distance;

                            recommendations[0] = 1;
                            recommendations[1] = i;
                        }

                        //Found another node at the same distance
                        else if(distance == shortestDistance){

                            recommendations[0]++;

                            recommendations[recommendations[0]] = i;
                        }
                    }
                }


                //Send recommendations back to reporting node
                MPI_Send(recommendations, size, MPI_INT,
                         nodeRank, BASE_RESPONSE_TAG,
                         MPI_COMM_WORLD);

                totalMessages++;


                //Print recommendations
                printf("Base station: Node %d recommendations: ",
                       nodeRank);

                if(recommendations[0] == 0){

                    printf("No nodes available nearby\n");

                }
                else{

                    for(int i = 1; i <= recommendations[0]; i++){
                        printf("%d ", recommendations[i]);
                    }

                    printf("\n");
                }


                //Log report information and computed nodes
                fprintf(logFile, "%.2f,REPORT,%d,%d,%d,%d,%d,\"",
                        MPI_Wtime() - startTime,
                        nodeRank,
                        receivedReport.Car.carID,
                        receivedReport.availableChargers,
                        totalAlerts,
                        totalMessages);

                if(recommendations[0] == 0){

                    fprintf(logFile, "None");

                }
                else{

                    for(int i = 1; i <= recommendations[0]; i++){

                        fprintf(logFile, "%d", recommendations[i]);

                        if(i < recommendations[0]){
                            fprintf(logFile, " ");
                        }
                    }
                }

                fprintf(logFile, "\"\n");
                fflush(logFile);
            }
        }


        //All nodes have stopped generating cars
        if(stopping && acknowledgements == numNodes){

            printf("Base station: All nodes ready for termination\n");

            //Tell all charging nodes to terminate
            for(int i = 0; i < numNodes; i++){

                int message = 1;

                MPI_Send(&message, 1, MPI_INT,
                         i, TERMINATE_TAG, MPI_COMM_WORLD);

                totalMessages++;
            }

            break;
        }

        usleep(1000);
    }


    //Final simulation results
    double totalTime = MPI_Wtime() - startTime;

    fprintf(logFile, "%.2f,SUMMARY,-1,-1,-1,%d,%d,\"Total events: %d\"\n",
            totalTime, totalAlerts, totalMessages, totalEvents);

    fflush(logFile);
    fclose(logFile);


    printf("\n===== BASE STATION SUMMARY =====\n");
    printf("Simulation time: %.2f seconds\n", totalTime);
    printf("Total alerts: %d\n", totalAlerts);
    printf("Total messages: %d\n", totalMessages);
    printf("Total events: %d\n", totalEvents);
    printf("Results written to base_station_log.csv\n");
    printf("================================\n");


    free(reportHistory);
    free(availabilityHistory);
    free(reportReceived);
    free(acknowledged);

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

    int baseStationRank = size - 1;
    int numNodes = size - 1;

    if (numNodes < 1) {

        if (rank == 0) {
            fprintf(stderr, "Need atleast 2 MPI Nodes\n");
        }

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == baseStationRank) {

        baseStation_process();

    }
    else{

        //Automatically determine grid dimensions
        int dims[2] = {0, 0};

        MPI_Dims_create(numNodes, 2, dims); //https://www.mpich.org/static/docs/v3.1/www3/MPI_Dims_create.html

        int N = dims[0];  //Rows
        int M = dims[1];  //Columns

        currNode.y = rank / M;
        currNode.x = rank % M;
        currNode.rank = rank;

        currNode.numChargers = NUM_OF_CHARGERS; //Change to dynamic range later TODO

        //Allocate charger array
        currNode.chargerStatus = (int*)malloc(currNode.numChargers * sizeof(int));

        if(currNode.chargerStatus == NULL){
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        //Initialise all chargers as free
        for(int i = 0; i < currNode.numChargers; i++){
            currNode.chargerStatus[i] = FREE;
        }


        //Populate Neighbours
        //Neighbour Logic. Null if no neighbour on that side
        //Neighbours: [Up, Down, Left, Right]
        currNode.neighbours[0] = (currNode.y > 0)? rank - M : MPI_PROC_NULL;
        currNode.neighbours[1] = (currNode.y < N - 1)? rank + M : MPI_PROC_NULL;
        currNode.neighbours[2] = (currNode.x > 0)? rank - 1 : MPI_PROC_NULL;
        currNode.neighbours[3] = (currNode.x < M - 1)? rank + 1 : MPI_PROC_NULL;


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

                charger_thread(threadID - 1,
                               currNode.chargerStatus,
                               currNode.rank);

            }
        }

        free(currNode.chargerStatus);
    }


    MPI_Finalize();

    return 0;
}

// mpicc -fopenmp task2.c -o task2   
// mpirun --oversubscribe -np <NUM_OF_NODES> ./task2  
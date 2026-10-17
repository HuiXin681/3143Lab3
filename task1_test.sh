#!/bin/bash

# MPI + OpenMP EV Charging Simulation Tests
# Original source file is not modified

set -u

SOURCE="task1_v2.c"
TEST_DIR="test_results"
NUM_PROCESSES=4
TEST_DURATION=12

mkdir -p "$TEST_DIR"

# Generate a temporary C file for each test case
create_test() {

    local test_num=$1
    local output="$TEST_DIR/test${test_num}.c"

    awk -v test="$test_num" '

    # Disable random charger updates during testing
    /charger_thread\(threadID/ {
        print "            while(1) sleep(1);"
        next
    }

    # Insert test initialisation before starting threads
    /\/\/Start threads/ {

        print "    //TEST INITIALISATION"

        # Set every charger to IN_USE by default
        print "    for(int i = 0; i < currNode.numChargers; i++){"
        print "        currNode.chargerStatus[i] = IN_USE;"
        print "    }"

        if(test == 1){

            # Mixed availability: Only Node 1 has free chargers
            print "    if(currNode.rank == 1){"
            print "        currNode.chargerStatus[0] = FREE;"
            print "        currNode.chargerStatus[1] = FREE;"
            print "    }"

        }
        else if(test == 3){

            # All chargers free
            print "    for(int i = 0; i < currNode.numChargers; i++){"
            print "        currNode.chargerStatus[i] = FREE;"
            print "    }"

        }

    }

    { print }

    ' "$SOURCE" > "$output"
}


# Compile and run one test
run_test() {

    local test_num=$1
    local executable="$TEST_DIR/test${test_num}"
    local logfile="$TEST_DIR/test${test_num}.log"

    echo ""
    echo "=========================================="
    echo "           RUNNING TEST $test_num"
    echo "=========================================="

    create_test "$test_num"

    # Compile
    mpicc -fopenmp "$TEST_DIR/test${test_num}.c" -o "$executable"

    if [ $? -ne 0 ]; then
        echo "FAIL: Compilation failed"
        return 1
    fi

    # Run simulation for 12 seconds
    # timeout stops the infinite while loops
    timeout -s INT "$TEST_DURATION" \
        mpirun --oversubscribe -np "$NUM_PROCESSES" \
        stdbuf -oL -eL "./$executable" \
        > "$logfile" 2>&1

    echo "Simulation completed."
    echo "Output saved to $logfile"

    # Check expected results
    case "$test_num" in

        1)
            echo ""
            echo "Checking mixed availability..."

            local passed=1

            grep -q "Node 0: Redirect EV to node 1" "$logfile" || passed=0
            grep -q "Node 3: Redirect EV to node 1" "$logfile" || passed=0
            grep -q "CONGESTION: Node 2" "$logfile" || passed=0

            if [ "$passed" -eq 1 ]; then
                echo "PASS: Neighbour redirection and congestion detection"
            else
                echo "FAIL: Expected communication not detected"
                echo "Check $logfile"
            fi
            ;;

        2)
            echo ""
            echo "Checking complete congestion..."

            local passed=1

            for rank in 0 1 2 3; do
                grep -q "CONGESTION: Node $rank" "$logfile" || passed=0
            done

            if [ "$passed" -eq 1 ]; then
                echo "PASS: All nodes detected congestion"
            else
                echo "FAIL: One or more nodes did not report congestion"
                echo "Check $logfile"
            fi
            ;;

        3)
            echo ""
            echo "Checking no congestion..."

            local passed=1

            # All nodes should report 4 available chargers
            for rank in 0 1 2 3; do
                grep -E "^\|Node $rank\|.*Available = 4" "$logfile" >/dev/null || passed=0
            done

            # No congestion or redirection should occur
            if grep -q "CONGESTION:" "$logfile"; then
                passed=0
            fi

            if grep -q "Redirect EV" "$logfile"; then
                passed=0
            fi

            if [ "$passed" -eq 1 ]; then
                echo "PASS: No unnecessary MPI requests"
            else
                echo "FAIL: Unexpected behaviour detected"
                echo "Check $logfile"
            fi
            ;;

    esac
}


# MAIN

echo "MPI EV Charging Simulation Test Suite"

if [ ! -f "$SOURCE" ]; then
    echo "ERROR: Cannot find $SOURCE"
    exit 1
fi

run_test 1
run_test 2
run_test 3

echo ""
echo "=========================================="
echo "           TESTING FINISHED"
echo "=========================================="
echo ""
echo "Logs saved in: $TEST_DIR/"
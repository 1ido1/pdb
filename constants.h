//
// Created by bayda on 13/02/2021.
//

#ifndef PDB_CONSTANTS_H
#define PDB_CONSTANTS_H

class Constants {
public:
    const static int BATCH_SIZE = 2;
    const static int EXECUTION_THREADS_NUMBER = 2;
    const static int CC_THREADS_NUMBER = 3;
    const static int NUM_THREADS = EXECUTION_THREADS_NUMBER + CC_THREADS_NUMBER;
    constexpr static double INITIALIZED_VALUE = -9999;
};

#endif //PDB_CONSTANTS_H

//
// Created by bayda on 13/02/2021.
//

#ifndef PDB_CONSTANTS_H
#define PDB_CONSTANTS_H

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

typedef tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> RecordsMap;
typedef std::shared_ptr<tbb::concurrent_unordered_map<int, std::shared_ptr<Record>>> RecordsMapPtr;

class Constants {
public:
    const static int BATCH_SIZE = 3;
    const static int EXECUTION_THREADS_NUMBER = 2;
    const static int CC_THREADS_NUMBER = 3;
    constexpr static double INITIALIZED_VALUE = -9999;
};

#endif //PDB_CONSTANTS_H

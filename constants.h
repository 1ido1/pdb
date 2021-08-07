//
// Created by bayda on 13/02/2021.
//

#ifndef PDB_CONSTANTS_H
#define PDB_CONSTANTS_H

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include "structures/record.h"

typedef tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> RecordsMap;
typedef std::shared_ptr<tbb::concurrent_unordered_map<int, std::shared_ptr<Record>>> RecordsMapPtr;

class Constants {
public:
    static std::string getEnvironmentVariableOrDefault(const std::string &variable_name,
                                                       const std::string &default_value);

    static int getIntEnvironmentVariableOrDefault(const std::string &variable_name,
                                                  int default_value);

    static const int BATCH_SIZE;
    static const int EXECUTION_THREADS_NUMBER;
    static const int CC_THREADS_NUMBER;
    constexpr static double INITIALIZED_VALUE = -9999;


};

#endif //PDB_CONSTANTS_H

//
// Created by bayda on 22/01/2021.
//

#ifndef PDB_CONCURRENCY_CONTROL_H
#define PDB_CONCURRENCY_CONTROL_H


#include <map>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include "record.h"
#include "constants.h"

class ConcurrencyControl {
private:
    tbb::concurrent_unordered_map<int,std::shared_ptr<Record>>& recordsMap;
    const tbb::concurrent_vector<std::shared_ptr<Transaction>>& logTransactions;
    int logPosition = 0;
    int threadNumber;

    bool isKeyInThePartition(int key);

public:
    ConcurrencyControl(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                       const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                       int threadNumber);
    void writeOperation(Operation operation, const Transaction &transaction);
    void writeTransaction(const Transaction &transaction);
    void readFromLog();
};


#endif //PDB_CONCURRENCY_CONTROL_H

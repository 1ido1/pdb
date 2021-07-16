//
// Created by bayda on 22/01/2021.
//

#ifndef PDB_CONCURRENCY_CONTROL_H
#define PDB_CONCURRENCY_CONTROL_H


#include <map>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <boost/thread/latch.hpp>
#include "structures/record.h"
#include "constants.h"

class ConcurrencyControl {
private:
    tbb::concurrent_unordered_map<int,std::shared_ptr<Record>>& recordsMap;
    const tbb::concurrent_vector<std::shared_ptr<Transaction>>& logTransactions;
    std::vector<std::shared_ptr<boost::latch>> &latches;
    int logPosition = 0;
    int threadNumber;
    int totalCCThreads;
    int batchSize;

    bool isKeyInThePartition(int key) const;

public:
    ConcurrencyControl(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                       const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                       std::vector<std::shared_ptr<boost::latch>> &latches,
                       int threadNumber,
                       int totalCCThreads,
                       int batchSize);
    void writeOperation(Operation operation, const Transaction &transaction);
    void writeTransaction(const Transaction &transaction);
    void readFromLog();
};


#endif //PDB_CONCURRENCY_CONTROL_H

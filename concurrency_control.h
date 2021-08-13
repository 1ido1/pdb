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
    RecordsMapPtr& recordsMap;
    const tbb::concurrent_vector<std::shared_ptr<Transaction>>& logTransactions;
    std::vector<std::shared_ptr<boost::latch>> &latches;
    int logPosition = 0;
    int threadNumber;
    int totalCCThreads;
    int batchSize;
    long batchNumber = 0;
    unsigned long logSize;

    bool isKeyInThePartition(long key) const;
    void readFromLogByBatchSize(int batchSize);
    void writeOperation(Operation operation, const Transaction &transaction);
    void writeTransaction(const Transaction &transaction);

public:
    ConcurrencyControl(
            RecordsMapPtr& recordsMap,
            const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
            std::vector<std::shared_ptr<boost::latch>> &latches,
            int threadNumber,
            int totalCCThreads,
            int batchSize,
            unsigned long logSize);
    void readFromLog();

};


#endif //PDB_CONCURRENCY_CONTROL_H


//
// Created by bayda on 12/02/2021.
//

#ifndef PDB_EXECUTION_H
#define PDB_EXECUTION_H

#include <map>
#include <memory>
#include <set>
#include <queue>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <boost/thread/latch.hpp>
#include "record.h"
#include "state.h"

class Execution {
private:
    tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap;
    const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions;
    tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState;
    std::vector<std::shared_ptr<boost::latch>> &latches;
    int threadNumber;
    int totalEThreads;
    int batchSize;
    int batchPosition = 0;
    std::queue<int> transactionsToExecute;

public:
    Execution(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
              const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
              tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
              std::vector<std::shared_ptr<boost::latch>> &latches,
              int threadNumber,
              int totalEThreads,
              int batchSize);

    void readFromLog();

    bool executeTransaction(long timestamp);

    bool executeReadOperation(Operation operation, long timestamp);

    bool isTimestampInRange(long timestamp, long beginTimestamp, long endTimestamp);

    bool executeUpdateOperation(Operation operation, long timestamp);

    bool executeInsertOperation(Operation operation);
};

#endif //PDB_EXECUTION_H

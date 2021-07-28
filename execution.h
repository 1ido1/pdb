
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
#include "structures/record.h"
#include "structures/state.h"
#include "constants.h"

class Execution {
private:
    std::vector<RecordsMapPtr> &recordsPartitionedByCct;
    const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions;
    tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState;
    std::vector<std::shared_ptr<boost::latch>> &latches;
    int threadNumber;
    int totalEThreads;
    int totalCCThreads;
    int batchSize;
    long logPosition = 0;
    long batchNumber = 0;
    std::queue<int> transactionsToExecute;

    void readFromLogByBatchSize(int batchSize);
    bool executeTransaction(long timestamp);
    double executeReadOperation(Operation operation, long timestamp);
    bool executeScanOperation(Operation operation, long timestamp);
    static bool isTimestampInRange(long timestamp, long beginTimestamp, long endTimestamp);
    bool executeUpdateOperation(Operation operation, long timestamp);
    bool executeInsertOperation(Operation operation);
    bool executeModifyOperation(Operation operation, long timestamp);
    double readValue(int key, long timestamp);

public:
    Execution(std::vector<RecordsMapPtr>& recordsMap,
              const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
              tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
              std::vector<std::shared_ptr<boost::latch>> &latches,
              int threadNumber,
              int totalEThreads,
              int totalCCThreads,
              int batchSize);

    void readFromLog();

};

#endif //PDB_EXECUTION_H

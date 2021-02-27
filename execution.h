
//
// Created by bayda on 12/02/2021.
//

#ifndef PDB_EXECUTION_H
#define PDB_EXECUTION_H

#include <map>
#include <memory>
#include <set>
#include <queue>
#include "record.h"
#include "state.h"

class Execution {
private:
    std::map<int, std::shared_ptr<Record>> &recordsMap;
    const std::vector<std::shared_ptr<Transaction>> &logTransactions;
    std::map<int, std::shared_ptr<TransactionState>> &timestampToTransactionState;
    int threadNumber;
    int batchPosition = 0;
    std::queue<int> transactionsToExecute;

public:
    Execution(std::map<int, std::shared_ptr<Record>> &recordsMap,
              const std::vector<std::shared_ptr<Transaction>> &logTransactions,
              std::map<int, std::shared_ptr<TransactionState>> &timestampToTransactionState, int threadNumber);

    void readFromLog();

    bool executeTransaction(int timestamp);

    bool executeReadOperation(Operation operation, long timestamp);

    bool isTimestampInRange(long timestamp, long beginTimestamp, long endTimestamp);

    bool executeUpdateOperation(Operation operation, long timestamp);

    bool executeInsertOperation(Operation operation);
};

#endif //PDB_EXECUTION_H

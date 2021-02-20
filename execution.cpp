//
// Created by bayda on 13/02/2021.
//

#include "execution.h"
#include "constants.h"

Execution::Execution(std::map<int, std::shared_ptr<Record>> &recordsMap,
                     const std::vector<std::shared_ptr<Transaction>> &logTransactions, int threadNumber)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          threadNumber(threadNumber) {}

void Execution::readFromLog() {
    while (batchPosition + Constants::BATCH_SIZE <= logTransactions.size()) {
        for (int i = threadNumber; i < Constants::BATCH_SIZE; i+=Constants::EXECUTION_THREADS_NUMBER) {
            transactionsToExecute.insert(i);
        }
    }
}



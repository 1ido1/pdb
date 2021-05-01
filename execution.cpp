//
// Created by bayda on 13/02/2021.
//

#include <iostream>
#include "execution.h"
#include "constants.h"
#include "state.h"

Execution::Execution(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                     const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                     tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
                     std::vector<std::shared_ptr<boost::latch>> &latches,
                     int threadNumber,
                     int totalEThreads,
                     int batchSize)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          timestampToTransactionState(timestampToTransactionState),
          latches(latches),
          threadNumber(threadNumber),
          totalEThreads(totalEThreads),
          batchSize(batchSize) {}

void Execution::readFromLog() {
    while (batchPosition + batchSize <= logTransactions.size()) {
        int batchNumber = batchPosition / batchSize;
        std::cout << "batchNumber in et " << batchNumber << std::endl;

        std::shared_ptr<boost::latch> &latch = latches.at(batchNumber);
        latch->wait();

        for (int i = threadNumber + batchPosition;
             i < batchPosition + batchSize; i += totalEThreads) {
            transactionsToExecute.push(i);
            std::cout << "thread number " << threadNumber << " transaction to execute " << i << std::endl;
        }
        batchPosition += batchSize;
        while (!transactionsToExecute.empty()) {
            int timestamp = transactionsToExecute.front();
            transactionsToExecute.pop();
            if (!executeTransaction(timestamp)) {
                transactionsToExecute.push(timestamp);
            }
        }
    }
}

bool Execution::executeTransaction(long timestamp) {
    //TODO: check this lock transaction
    std::shared_ptr<TransactionState> &state = timestampToTransactionState[timestamp];
    auto currentState = &state;
    if (**currentState != TransactionState::unprocessed) {
        return false;
    }

    auto desiredState = std::make_shared<TransactionState>(TransactionState::executing);

    bool succ = std::atomic_compare_exchange_weak(&state, currentState, desiredState);
    if (!succ) {
        return false;
    }
    std::shared_ptr<Transaction> transaction = logTransactions.at(timestamp);
    for (auto operation : transaction->operations) {
        bool success;
        switch (operation.inputType) {
            case InputTypes::read:
                success = executeReadOperation(operation, transaction->timestamp);
                break;
            case InputTypes::update:
                success = executeUpdateOperation(operation, transaction->timestamp);
                break;
            case InputTypes::insert:
                success = executeInsertOperation(operation);
                break;
            //TODO: implement modify and scan
            case InputTypes::modify:
                break;
            case InputTypes::scan:
                break;

        }

        if (!success) {
            return false;
        }
    }
    return true;


}

bool Execution::executeReadOperation(Operation operation, long timestamp) {
    std::shared_ptr<Record> &record = recordsMap.at(operation.key);
    while (!isTimestampInRange(timestamp, record->beginTimestamp, record->endTimestamp)) {
        record = record->prev;
//        if (record == nullptr) {
//            return false;
//        }
    }
    std::cout << "record value " << record->value;

    if (record->value == Constants::INITIALIZED_VALUE) {
        return executeTransaction(record->transaction.timestamp);
    }

    return true;
}

bool Execution::isTimestampInRange(long timestamp, long beginTimestamp, long endTimestamp) {
    if (timestamp >= beginTimestamp && timestamp < endTimestamp) {
        return true;
    }
    return false;
}

bool Execution::executeUpdateOperation(Operation operation, long timestamp) {
    std::shared_ptr<Record> &record = recordsMap.at(operation.key);
    while (!isTimestampInRange(timestamp, record->beginTimestamp, record->endTimestamp)) {
        record = record->prev;
    }
    record->value = operation.value;
    std::cout << "updated record value " << record->value;

    return true;
}

bool Execution::executeInsertOperation(Operation operation) {
    std::shared_ptr<Record> &record = recordsMap.at(operation.key);
    if (record->value != Constants::INITIALIZED_VALUE) {
        //it depend on how you do the recovery in case failing in completing a transaction
        std::cout << "fatal error, insert should be the first value";

    }
    record->value = operation.value;

    return true;
}



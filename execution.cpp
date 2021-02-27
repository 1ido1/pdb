//
// Created by bayda on 13/02/2021.
//

#include "execution.h"
#include "constants.h"
#include "state.h"

Execution::Execution(std::map<int, std::shared_ptr<Record>> &recordsMap,
                     const std::vector<std::shared_ptr<Transaction>> &logTransactions,
                     std::map<int, std::shared_ptr<TransactionState>> &timestampToTransactionState, int threadNumber)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          timestampToTransactionState(timestampToTransactionState),
          threadNumber(threadNumber) {}

void Execution::readFromLog() {
    while (batchPosition + Constants::BATCH_SIZE <= logTransactions.size()) {
        for (int i = threadNumber + batchPosition;
             i < Constants::BATCH_SIZE; i += Constants::EXECUTION_THREADS_NUMBER) {
            transactionsToExecute.push(i);
        }
        batchPosition += Constants::EXECUTION_THREADS_NUMBER;
        while (!transactionsToExecute.empty()) {
            int timestamp = transactionsToExecute.front();
            transactionsToExecute.pop();
            if (!executeTransaction(timestamp)) {
                transactionsToExecute.push(timestamp);
            }
        }
    }
}

bool Execution::executeTransaction(int timestamp) {
    //TODO: lock transaction
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



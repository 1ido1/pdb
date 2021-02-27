//
// Created by bayda on 22/01/2021.
//

#include <climits>
#include <memory>
#include "concurrency_control.h"

ConcurrencyControl::ConcurrencyControl(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                                       const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                                       int threadNumber)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          threadNumber(threadNumber) {}

void ConcurrencyControl::writeOperation(Operation operation, const Transaction &transaction) {
    if (recordsMap.count(operation.key)) {
        auto prevRecord = recordsMap.at(operation.key);
        prevRecord->endTimestamp = transaction.timestamp;
        auto newRecord =
                std::make_shared<Record>(transaction.timestamp, LONG_MAX, transaction, Constants::INITIALIZED_VALUE,
                                         prevRecord);
        recordsMap.emplace(operation.key, newRecord);
    } else {
        Record record(transaction.timestamp, LONG_MAX, transaction, Constants::INITIALIZED_VALUE, nullptr);
        recordsMap.emplace(operation.key, std::make_shared<Record>(record));
    }
}

void ConcurrencyControl::writeTransaction(const Transaction &transaction) {
    for (Operation operation : transaction.operations) {
        if (!isKeyInThePartition(operation.key)) {
            continue;
        }

        if (operation.inputType == InputTypes::insert || operation.inputType == InputTypes::modify ||
            operation.inputType == InputTypes::update) {
            writeOperation(operation, transaction);
        }
    }
}

void ConcurrencyControl::readFromLog() {
    while (logPosition + Constants::BATCH_SIZE <= logTransactions.size()) {
        for (int i = 0; i < Constants::BATCH_SIZE; ++i) {
            writeTransaction(*logTransactions.at(logPosition++));
        }

        //TODO: wait for all other to finish
    }
}

bool ConcurrencyControl::isKeyInThePartition(int key) {
    return key % Constants::CC_THREADS_NUMBER == threadNumber;
}

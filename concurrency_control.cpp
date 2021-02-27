//
// Created by bayda on 22/01/2021.
//

#include <climits>
#include <memory>
#include "concurrency_control.h"

ConcurrencyControl::ConcurrencyControl(std::map<int, std::shared_ptr<Record>> &recordsMap,
                                       const std::vector<std::shared_ptr<Transaction>> &logTransactions)
        : recordsMap(recordsMap),
          logTransactions(logTransactions) {}

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

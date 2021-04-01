//
// Created by bayda on 22/01/2021.
//

#include <climits>
#include <memory>
#include "concurrency_control.h"

ConcurrencyControl::ConcurrencyControl(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                                       const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                                       std::vector<std::shared_ptr<boost::latch>> &latches,
                                       int threadNumber)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          latches(latches),
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
//        std::cout << "cc thread " << threadNumber << " operation.key " << operation.key << " isKeyInThePartition " << isKeyInThePartition(operation.key) << std::endl;
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

        int batchNumber = logPosition / Constants::BATCH_SIZE - 1;
        std::cout << "batchNumber in cc " << batchNumber << std::endl;
        std::shared_ptr<boost::latch> &latch = latches.at(batchNumber);
        latch->count_down_and_wait();
    }
}

bool ConcurrencyControl::isKeyInThePartition(int key) const {
    return std::hash<int>{}(key) % Constants::CC_THREADS_NUMBER == threadNumber;
}

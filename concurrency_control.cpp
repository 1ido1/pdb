//
// Created by bayda on 22/01/2021.
//

#include <climits>
#include <memory>
#include <iostream>
#include "spdlog/spdlog.h"
#include "spdlog/fmt/ostr.h"
#include "concurrency_control.h"
#include "utils.h"

ConcurrencyControl::ConcurrencyControl(
        RecordsMapPtr &recordsMap,
        const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
        std::vector<std::shared_ptr<boost::latch>> &latches,
        int threadNumber,
        int totalCCThreads,
        int batchSize)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          latches(latches),
          threadNumber(threadNumber),
          totalCCThreads(totalCCThreads),
          batchSize(batchSize) {}

void ConcurrencyControl::writeOperation(Operation operation, const Transaction &transaction) {
    spdlog::debug("writing operation {}, timestamp {} in thread {}",
                  operation, transaction.timestamp, threadNumber);

    if (recordsMap->count(operation.key)) {
        auto prevRecord = recordsMap->at(operation.key);
        prevRecord->endTimestamp = transaction.timestamp;
        auto newRecord =
                std::make_shared<Record>(transaction.timestamp, LONG_MAX, transaction, Constants::INITIALIZED_VALUE,
                                         prevRecord);
        (*recordsMap)[operation.key] = newRecord;
        spdlog::info("adding new record for key {}, timestamp {} in thread {}",
                     newRecord, transaction.timestamp, threadNumber);
    } else {
        Record record(transaction.timestamp, LONG_MAX, transaction, Constants::INITIALIZED_VALUE, nullptr);
        recordsMap->emplace(operation.key, std::make_shared<Record>(record));
        spdlog::info("writing first record for key {}, timestamp {} in thread {}",
                     record, transaction.timestamp, threadNumber);
    }
}

void ConcurrencyControl::writeTransaction(const Transaction &transaction) {
    spdlog::debug("writing transaction {}", transaction);

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

void ConcurrencyControl::readFromLogByBatchSize(int batchSize) {
    while (logPosition + batchSize <= logTransactions.size()) {
        for (int i = 0; i < batchSize; ++i) {
            writeTransaction(*logTransactions.at(logPosition++));
        }

        std::shared_ptr<boost::latch> &latch = latches.at(batchNumber++);
        latch->count_down_and_wait();
    }
}

void ConcurrencyControl::readFromLog() {
    readFromLogByBatchSize(batchSize);
    // last batch
    unsigned long remainder = logTransactions.size() - logPosition;
    if (remainder > 0) {
        readFromLogByBatchSize(remainder);
    }
}

//TODO: check if need to change the hash function
bool ConcurrencyControl::isKeyInThePartition(int key) const {
    return Utils::getCcThreadNumber(key, totalCCThreads) == threadNumber;
}

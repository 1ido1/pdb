//
// Created by bayda on 22/01/2021.
//

#include <climits>
#include <memory>
#include "spdlog/spdlog.h"
#include "spdlog/fmt/ostr.h"
#include "concurrency_control.h"
#include "utils.h"

ConcurrencyControl::ConcurrencyControl(
        RecordsMapPtr &recordsMap,
        const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
        std::vector<std::shared_ptr<boost::latch>> &latches,
        long logPosition,
        int threadNumber,
        int totalCCThreads,
        int batchSize,
        unsigned long logSize)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          latches(latches),
          logPosition(logPosition),
          threadNumber(threadNumber),
          totalCCThreads(totalCCThreads),
          batchSize(batchSize),
          logSize(logSize),
          batchNumber((logPosition + batchSize - 1) / batchSize)
          {}

ConcurrencyControl::ConcurrencyControl(
        RecordsMapPtr &recordsMap,
        const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
        std::vector<std::shared_ptr<boost::latch>> &latches,
        int threadNumber,
        int totalCCThreads,
        int batchSize,
        unsigned long logSize)
        : recordsMap(recordsMap),
          logTransactions(logTransactions),
          latches(latches),
          threadNumber(threadNumber),
          totalCCThreads(totalCCThreads),
          batchSize(batchSize),
          logSize(logSize) {}


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
                     operation.key, transaction.timestamp, threadNumber);
    } else {
        Record record(transaction.timestamp, LONG_MAX, transaction, Constants::INITIALIZED_VALUE, nullptr);
        recordsMap->emplace(operation.key, std::make_shared<Record>(record));
        spdlog::info("writing first record for key {}, timestamp {} in thread {}",
                     operation.key, transaction.timestamp, threadNumber);
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
    while (logPosition + batchSize <= logSize) {
        if (logPosition + batchSize > logTransactions.size()) {
            spdlog::info("waiting for log writer to write");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        for (int i = 0; i < batchSize; ++i) {
            writeTransaction(*logTransactions.at(logPosition++));
        }

        std::shared_ptr<boost::latch> &latch = latches.at(batchNumber++);
        latch->count_down_and_wait();
    }
}

void ConcurrencyControl::readFromLog() {
    spdlog::info("Start reading log at thread {}, logTransactions.size() {}", threadNumber, logTransactions.size());
    readFromLogByBatchSize(batchSize);
    // last batch
    unsigned long remainder = logSize - logPosition;
    if (remainder > 0) {
        readFromLogByBatchSize(remainder);
    }
}

//TODO: check if need to change the hash function
bool ConcurrencyControl::isKeyInThePartition(long key) const {
    return Utils::getCcThreadNumber(key, totalCCThreads) == threadNumber;
}

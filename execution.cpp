//
// Created by bayda on 13/02/2021.
//

#include <iostream>
#include "spdlog/spdlog.h"
#include "spdlog/fmt/ostr.h"
#include "execution.h"
#include "constants.h"
#include "structures/state.h"
#include "utils.h"

Execution::Execution(
        std::vector<RecordsMapPtr>
        &recordsPartitionedByCct,
        const tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
        std::vector<std::shared_ptr<boost::latch>> &latches,
        int threadNumber,
        int totalEThreads,
        int totalCCThreads,
        int batchSize)
        : recordsPartitionedByCct(recordsPartitionedByCct),
          logTransactions(logTransactions),
          timestampToTransactionState(timestampToTransactionState),
          latches(latches),
          threadNumber(threadNumber),
          totalEThreads(totalEThreads),
          totalCCThreads(totalCCThreads),
          batchSize(batchSize) {}

void Execution::readFromLog() {
    readFromLogByBatchSize(batchSize);
    // last batch
    unsigned long remainder = logTransactions.size() - logPosition;
    if (remainder > 0) {
        readFromLogByBatchSize(remainder);
    }
}

void Execution::readFromLogByBatchSize(int batchSize) {
    while (logPosition + batchSize <= logTransactions.size()) {
        spdlog::info("batch {} in execution thread {}", batchNumber, threadNumber);

        std::shared_ptr<boost::latch> &latch = latches.at(batchNumber++);
        latch->wait();

        for (int i = threadNumber + logPosition;
             i < logPosition + batchSize; i += totalEThreads) {
            transactionsToExecute.push(i);
            spdlog::info("thread number {}, transaction to execute {}", threadNumber, i);
        }
        logPosition += batchSize;
        while (!transactionsToExecute.empty()) {
            long timestamp = transactionsToExecute.front();
            transactionsToExecute.pop();
            if (!executeTransaction(timestamp)) {
                transactionsToExecute.push(timestamp);
            }
        }
    }
}

bool Execution::executeTransaction(long timestamp) {
    std::shared_ptr<TransactionState> &state = timestampToTransactionState[timestamp];
    auto currentState = &state;
    if (**currentState == TransactionState::done) {
        return true;
    }

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
                success = executeReadOperation(operation, transaction->timestamp) != Constants::INITIALIZED_VALUE;
                break;
            case InputTypes::update:
                success = executeUpdateOperation(operation, transaction->timestamp);
                break;
            case InputTypes::insert:
                success = executeInsertOperation(operation);
                break;
            case InputTypes::modify:
                success = executeModifyOperation(operation, timestamp);
                break;
            case InputTypes::scan:
                success = executeScanOperation(operation, timestamp);
                break;
        }

        if (!success) {
            timestampToTransactionState[timestamp] = std::make_shared<TransactionState>(TransactionState::unprocessed);
            return false;
        }
    }

    timestampToTransactionState[timestamp] = std::make_shared<TransactionState>(TransactionState::done);
    return true;


}

double Execution::executeReadOperation(Operation operation, long timestamp) {
    return readValue(operation.key, timestamp);
}

double Execution::readValue(long key, long timestamp) {
    int ccThreadNumber = Utils::getCcThreadNumber(key, totalCCThreads);
    std::shared_ptr<Record> record = recordsPartitionedByCct.at(ccThreadNumber)->at(key);
    while (!isTimestampInRange(timestamp, record->beginTimestamp, record->endTimestamp)) {
        record = record->prev;
        if (record == nullptr) {
            spdlog::error("could not find the correct value, key {}, timestamp {}",
                          key, timestamp);
            return Constants::INITIALIZED_VALUE;
        }
    }


    if (record->value != Constants::INITIALIZED_VALUE) {
        spdlog::info("read operation: record value {} in thread {}", record->value, threadNumber);
        return record->value;
    }

    spdlog::debug("record still wasn't processed , key {}, timestamp {}",
                  key, timestamp);

    if (executeTransaction(record->transaction.timestamp)) {
        assert(record->value != Constants::INITIALIZED_VALUE);
        spdlog::info("read operation: record value {} in thread {}", record->value, threadNumber);
        return record->value;
    }

    return Constants::INITIALIZED_VALUE;
}

bool Execution::isTimestampInRange(long timestamp, long beginTimestamp, long endTimestamp) {
    if (timestamp >= beginTimestamp && timestamp < endTimestamp) {
        return true;
    }
    return false;
}

bool Execution::executeUpdateOperation(Operation operation, long timestamp) {
    int ccThreadNumber = Utils::getCcThreadNumber(operation.key, totalCCThreads);
    std::shared_ptr<Record> record = recordsPartitionedByCct.at(
            ccThreadNumber)->at(operation.key);

    while (!isTimestampInRange(timestamp, record->beginTimestamp, record->endTimestamp)) {
        record = record->prev;
    }
    record->value = operation.value;
    spdlog::info("updated record value {} for timestamp {} in thread {}", record->value, timestamp, threadNumber);

    return true;
}

bool Execution::executeInsertOperation(Operation operation) {
    int ccThreadNumber = Utils::getCcThreadNumber(operation.key, totalCCThreads);

    std::shared_ptr<Record> record = recordsPartitionedByCct.at(ccThreadNumber)->at(operation.key);

    while (record->prev != nullptr) {
        record = record->prev;
    }

    if (record->value != Constants::INITIALIZED_VALUE) {
        //it depend on how you do the recovery in case failing in completing a transaction
        spdlog::error("insert should be the first value, operation: {}", operation);
    }

    spdlog::debug("writing insert operation {} in thread {}", operation, threadNumber);
    record->value = operation.value;

    return true;
}

bool Execution::executeModifyOperation(Operation operation, long timestamp) {
    double value = readValue(operation.key, timestamp - 1);
    if (value == Constants::INITIALIZED_VALUE) {
        return false;
    }

    int ccThreadNumber = Utils::getCcThreadNumber(operation.key, totalCCThreads);
    std::shared_ptr<Record> record = recordsPartitionedByCct.at(ccThreadNumber)->at(operation.key);
    while (!isTimestampInRange(timestamp, record->beginTimestamp, record->endTimestamp)) {
        record = record->prev;
    }
    std::shared_ptr<Record> &pervRecord = record->prev;

    if (pervRecord->value == operation.value) {
        record->value = operation.value + 1;
    } else {
        record->value = pervRecord->value;
    }

    spdlog::debug("updated record value {} for timestamp {} in thread {}", record->value, timestamp, threadNumber);

    return true;
}

bool Execution::executeScanOperation(Operation operation, long timestamp) {
    spdlog::debug("scan operation, operation: ", operation);

    double value;
    for (int i = 0; i < operation.range; ++i) {
        value = readValue(operation.key + i, timestamp);
        if (value == Constants::INITIALIZED_VALUE) {
            return false;
        }
    }
    return true;
}



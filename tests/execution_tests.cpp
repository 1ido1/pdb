//
// Created by bayda on 29/04/2021.
//

#include "gtest/gtest.h"
#include <vector>
#include <memory>
#include "../structures/input.h"
#include "../concurrency_control.h"
#include "../execution.h"
#include "../utils.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <boost/thread/latch.hpp>

namespace {

    void executeEThreads(tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> &recordsMap,
                         tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                         tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
                         std::vector<std::shared_ptr<boost::latch>> &latches,
                         int eThreadsNumber,
                         int batchSize) {
        std::vector<std::thread> eThreads = Utils::startEThreads(
                recordsMap, logTransactions, timestampToTransactionState,
                latches, eThreadsNumber, batchSize);

        for (auto &th : eThreads) {
            th.join();
        }
    }

    tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>>
    initIdToTransactionState(int logTransactionsSize) {
        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState;
        for (int i = 0; i < logTransactionsSize; ++i) {
            idToTransactionState[i] = std::make_shared<TransactionState>(TransactionState::unprocessed);
        }

        return idToTransactionState;
    }

    std::vector<std::shared_ptr<boost::latch>> initLatchesForExecution(int latchesSize) {
        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(1, latchesSize);
        for (int i = 0; i < latchesSize; ++i) {
            latches.at(i)->count_down();
        }

        return latches;
    }

    TEST(ExcutionTests, InsertOperation) {
        const int eThreadsNumber = 1;
        const int batchSize = 1;

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> operation{Operation{InputTypes::insert, 0, 2133}
        };

        const std::shared_ptr<Transaction> &transaction = std::make_shared<Transaction>(operation, 0);
        logTransactions.push_back(transaction);
        Record record{0, LONG_MAX, *transaction, -9999, nullptr};
        recordsMap[0] = std::make_shared<Record>(record);

        std::vector<std::shared_ptr<boost::latch>> latches =
                initLatchesForExecution(logTransactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState =
                initIdToTransactionState(logTransactions.size());

        Record expectedRecord{0, LONG_MAX, *transaction, 2133, nullptr};

        executeEThreads(recordsMap, logTransactions, idToTransactionState,
                        latches, eThreadsNumber, batchSize);

        EXPECT_EQ(recordsMap.size(), 1);
        EXPECT_EQ(*recordsMap[0].get(), expectedRecord);
    }

    TEST(ExcutionTests, UpdateOperation) {
        const int eThreadsNumber = 1;
        const int batchSize = 2;

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> insertOperation{Operation{InputTypes::insert, 0, 1}
        };
        std::vector<Operation> updateOperation{Operation{InputTypes::update, 0, 2}
        };

        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(insertOperation, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(updateOperation, 1);

        logTransactions.push_back(transaction1);
        logTransactions.push_back(transaction2);

        Record firstRecord{0, 1, *transaction1, Constants::INITIALIZED_VALUE, nullptr};
        Record updatedRecord{1, LONG_MAX, *transaction2,
                             Constants::INITIALIZED_VALUE, std::make_shared<Record>(firstRecord)};
        recordsMap[0] = std::make_shared<Record>(updatedRecord);

        std::vector<std::shared_ptr<boost::latch>> latches =
                initLatchesForExecution(logTransactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState =
                initIdToTransactionState(logTransactions.size());

        Record expectedFirstRecord{0, 1, *transaction1, 1, nullptr};
        Record expectedUpdatedRecord{1, LONG_MAX, *transaction2,
                                     2, std::make_shared<Record>(expectedFirstRecord)};

        executeEThreads(recordsMap, logTransactions, idToTransactionState,
                        latches, eThreadsNumber, batchSize);

        EXPECT_EQ(recordsMap.size(), 1);
        EXPECT_EQ(*recordsMap[0].get(), expectedUpdatedRecord);
    }

    TEST(ExcutionTests, ReadLeadToWriteTest) {
        const int eThreadsNumber = 2;
        const int batchSize = 2;

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> insertOperation{Operation{InputTypes::insert, 0, 1}
        };
        std::vector<Operation> readOperation{Operation{InputTypes::read, 0}
        };

        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(insertOperation, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(readOperation, 1);

        logTransactions.push_back(transaction1);
        logTransactions.push_back(transaction2);

        Record firstRecord{0, LONG_MAX, *transaction1, Constants::INITIALIZED_VALUE, nullptr};
        recordsMap[0] = std::make_shared<Record>(firstRecord);

        std::vector<std::shared_ptr<boost::latch>> latches =
                initLatchesForExecution(logTransactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState =
                initIdToTransactionState(logTransactions.size());

        Record expectedRecord{0, LONG_MAX, *transaction1, 1, nullptr};

        Execution execution{recordsMap, logTransactions,
                            idToTransactionState, latches, 1, eThreadsNumber, batchSize};
        execution.readFromLog();

        EXPECT_EQ(recordsMap.size(), 1);
        EXPECT_EQ(*recordsMap[0].get(), expectedRecord);
    }

    TEST(ExcutionTests, ReadModifyWriteTest) {
        const int eThreadsNumber = 1;
        const int batchSize = 2;

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> insertOperation{Operation{InputTypes::insert, 0, 1}
        };
        std::vector<Operation> modifyOperation{Operation{InputTypes::modify, 0, 1}
        };

        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(insertOperation, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(modifyOperation, 1);

        logTransactions.push_back(transaction1);
        logTransactions.push_back(transaction2);

        Record firstRecord{0, 1, *transaction1, Constants::INITIALIZED_VALUE, nullptr};
        Record updatedRecord{1, LONG_MAX, *transaction2,
                             Constants::INITIALIZED_VALUE, std::make_shared<Record>(firstRecord)};
        recordsMap[0] = std::make_shared<Record>(updatedRecord);

        std::vector<std::shared_ptr<boost::latch>> latches =
                initLatchesForExecution(logTransactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState =
                initIdToTransactionState(logTransactions.size());


        Record expectedFirstRecord{0, 1, *transaction1, 1, nullptr};
        Record expectedRecord{1, LONG_MAX, *transaction2,
                              2, std::make_shared<Record>(expectedFirstRecord)};

        executeEThreads(recordsMap, logTransactions, idToTransactionState,
                        latches, eThreadsNumber, batchSize);

        EXPECT_EQ(recordsMap.size(), 1);
        EXPECT_EQ(*recordsMap[0].get(), expectedRecord);
    }

    TEST(ExcutionTests, TwoThreads) {
        const int eThreadsNumber = 2;
        const int batchSize = 2;

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> operation1{Operation{InputTypes::insert, 0, 2133}
        };
        std::vector<Operation> operation2{Operation{InputTypes::insert, 5, 435},
                                          Operation{InputTypes::insert, 3, 763}
        };

        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(operation1, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(operation2, 1);
        logTransactions.push_back(transaction1);
        logTransactions.push_back(transaction2);

        Record record1{0, LONG_MAX, *transaction1, -9999, nullptr};
        Record record2{1, LONG_MAX, *transaction2, -9999, nullptr};
        Record record3{1, LONG_MAX, *transaction2, -9999, nullptr};
        recordsMap[0] = std::make_shared<Record>(record1);
        recordsMap[5] = std::make_shared<Record>(record2);
        recordsMap[3] = std::make_shared<Record>(record3);

        std::vector<std::shared_ptr<boost::latch>> latches =
                initLatchesForExecution(logTransactions.size() / batchSize + 1);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> idToTransactionState =
                initIdToTransactionState(logTransactions.size());


        Record expectedRecord1{0, LONG_MAX, *transaction1, 2133, nullptr};
        Record expectedRecord2{1, LONG_MAX, *transaction2, 435, nullptr};
        Record expectedRecord3{1, LONG_MAX, *transaction2, 763, nullptr};

        executeEThreads(recordsMap, logTransactions, idToTransactionState,
                        latches, eThreadsNumber, batchSize);

        EXPECT_EQ(recordsMap.size(), 3);
        EXPECT_EQ(*recordsMap[0].get(), expectedRecord1);
        EXPECT_EQ(*recordsMap[5].get(), expectedRecord2);
        EXPECT_EQ(*recordsMap[3].get(), expectedRecord3);

    }
}
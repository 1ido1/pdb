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

    void executeCCThreads(std::vector<RecordsMapPtr> &recordsPartitionedByCct,
                          tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                          std::vector<std::shared_ptr<boost::latch>> &latches,
                          const int ccThreadsNumber, const int batchSize) {
        std::vector<std::thread> ccThreads = Utils::startCCThreads(
                latches, logTransactions, recordsPartitionedByCct,
                ccThreadsNumber, batchSize, logTransactions.size());

        for (auto &th : ccThreads) {
            th.join();
        }
    }

    TEST(CCTests, InsertOperation) {
        const int ccThreadsNumber = 1;
        const int batchSize = 1;

        std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);
        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        std::vector<Operation> operation{Operation{InputTypes::insert, 0, 2133}
        };

        const std::shared_ptr<Transaction> &transaction = std::make_shared<Transaction>(operation, 0);
        logTransactions.push_back(transaction);
        Record expectedRecord{0, LONG_MAX, *transaction, -9999, nullptr};

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, logTransactions.size() / batchSize);
        executeCCThreads(recordsPartitionedByCct, logTransactions, latches, ccThreadsNumber, batchSize);

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 1);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 0, ccThreadsNumber), expectedRecord);
    }


    TEST(CCTests, UpdateOperation) {
        const int ccThreadsNumber = 1;
        const int batchSize = 2;

        std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);
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
        Record expectedRecord{1, LONG_MAX, *transaction2,
                              Constants::INITIALIZED_VALUE, std::make_shared<Record>(firstRecord)};

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, logTransactions.size() / batchSize);
        executeCCThreads(recordsPartitionedByCct, logTransactions, latches, ccThreadsNumber, batchSize);

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 1);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 0, ccThreadsNumber), expectedRecord);
    }

    TEST(CCTests, TwoThreads) {
        const int ccThreadsNumber = 2;
        const int batchSize = 2;

        std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);
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

        Record expectedRecord1{0, LONG_MAX, *transaction1, -9999, nullptr};
        Record expectedRecord2{1, LONG_MAX, *transaction2, -9999, nullptr};
        Record expectedRecord3{1, LONG_MAX, *transaction2, -9999, nullptr};

        std::vector<std::shared_ptr<boost::latch>> latches = Utils::initLatches(ccThreadsNumber,
                                                                                logTransactions.size() / batchSize);
        executeCCThreads(recordsPartitionedByCct, logTransactions, latches, ccThreadsNumber, batchSize);

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 3);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 0, ccThreadsNumber), expectedRecord1);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 5, ccThreadsNumber), expectedRecord2);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 3, ccThreadsNumber), expectedRecord3);
    }

}
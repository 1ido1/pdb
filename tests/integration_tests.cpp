//
// Created by bayda on 17/04/2021.
//

#include "gtest/gtest.h"
#include <vector>
#include <memory>
#include "../structures/input.h"
#include "../log_writer.h"
#include "../concurrency_control.h"
#include "../execution.h"
#include "../utils.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <boost/thread/latch.hpp>


namespace {
    Record createUpdatedRecord(double insertedValue,
                               double updatedValue,
                               const std::shared_ptr<Transaction> &transaction1,
                               const std::shared_ptr<Transaction> &transaction2) {
        Record expectedFirstRecord{0, 1, *transaction1, insertedValue, nullptr};
        Record expectedUpdatedRecord{1, LONG_MAX, *transaction2,
                                     updatedValue, std::make_shared<Record>(expectedFirstRecord)};

        return expectedUpdatedRecord;

    }

    TEST(IntegrationTest, InsertTest) {
        int ccThreadsNumber = 3;
        int eThreadsNumber = 2;
        int batchSize = 2;
        std::vector<std::shared_ptr<Transaction>> transactions{};
        std::vector<Operation> operations1{Operation{InputTypes::insert, 0, 2133},
                                           Operation{InputTypes::insert, 4, 345345},
                                           Operation{InputTypes::insert, 3, 45345},
                                           Operation{InputTypes::insert, 77, 3421},
                                           Operation{InputTypes::insert, 81, 5333},
        };

        std::vector<Operation> operations2{Operation{InputTypes::insert, 778, 2133},
                                           Operation{InputTypes::insert, 54, 345345},
                                           Operation{InputTypes::insert, 42, 45345},
                                           Operation{InputTypes::insert, 12, 3421},
                                           Operation{InputTypes::insert, 32, 5333},
        };


        transactions.push_back(std::make_shared<Transaction>(operations1, 0));
        transactions.push_back(std::make_shared<Transaction>(operations2, 1));

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, transactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
                Utils::initTimestampToTransactionState(transactions.size());

        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        LogWriter logWriter{logTransactions};
        logWriter.writeLog(transactions);

        std::vector<RecordsMapPtr>
                recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);

        std::vector<std::thread> ccThreads = Utils::startCCThreads(
                latches, logTransactions, recordsPartitionedByCct,
                ccThreadsNumber, batchSize, transactions.size());

        std::vector<std::thread> eThreads = Utils::startEThreads(
                recordsPartitionedByCct, logTransactions, timestampToTransactionState,
                latches, eThreadsNumber, ccThreadsNumber, batchSize
        );

        for (auto &th : ccThreads) {
            th.join();
        }

        for (auto &th : eThreads) {
            th.join();
        }

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 10);
    }

    TEST(IntegrationTest, UpdateTest) {
        int ccThreadsNumber = 3;
        int eThreadsNumber = 2;
        int batchSize = 2;
        std::vector<std::shared_ptr<Transaction>> transactions{};
        std::vector<Operation> operations1{Operation{InputTypes::insert, 0, 2133},
                                           Operation{InputTypes::insert, 4, 345345},
                                           Operation{InputTypes::insert, 3, 45345},
                                           Operation{InputTypes::insert, 77, 3421},
                                           Operation{InputTypes::insert, 81, 5333},
        };

        std::vector<Operation> operations2{Operation{InputTypes::update, 0, 6754},
                                           Operation{InputTypes::update, 4, 2345},
                                           Operation{InputTypes::update, 3, 45345},
                                           Operation{InputTypes::update, 77, 865},
                                           Operation{InputTypes::update, 81, 88856},
        };


        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(operations1, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(operations2, 1);
        transactions.push_back(transaction1);
        transactions.push_back(transaction2);

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, transactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
                Utils::initTimestampToTransactionState(transactions.size());

        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        LogWriter logWriter{logTransactions};
        logWriter.writeLog(transactions);

        std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);

        Record expectedRecord1 = createUpdatedRecord(2133, 6754, transaction1, transaction2);
        Record expectedRecord2 = createUpdatedRecord(345345, 2345, transaction1, transaction2);
        Record expectedRecord3 = createUpdatedRecord(45345, 45345, transaction1, transaction2);
        Record expectedRecord4 = createUpdatedRecord(3421, 865, transaction1, transaction2);
        Record expectedRecord5 = createUpdatedRecord(5333, 88856, transaction1, transaction2);

        std::vector<std::thread> ccThreads = Utils::startCCThreads(
                latches, logTransactions, recordsPartitionedByCct,
                ccThreadsNumber, batchSize, transactions.size());

        std::vector<std::thread> eThreads = Utils::startEThreads(
                recordsPartitionedByCct, logTransactions, timestampToTransactionState,
                latches, eThreadsNumber, ccThreadsNumber, batchSize
        );

        for (auto &th : ccThreads) {
            th.join();
        }

        for (auto &th : eThreads) {
            th.join();
        }

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 5);

        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 0, ccThreadsNumber), expectedRecord1);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 4, ccThreadsNumber), expectedRecord2);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 3, ccThreadsNumber), expectedRecord3);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 77, ccThreadsNumber), expectedRecord4);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 81, ccThreadsNumber), expectedRecord5);
    }


    TEST(IntegrationTest, TransactionsSizeDivdedByBatchSizeWithRemainder) {
        spdlog::set_level(spdlog::level::debug);
        const int ccThreadsNumber = 2;
        const int eThreadsNumber = 2;
        const int batchSize = 2;
        std::vector<std::shared_ptr<Transaction>> transactions{};
        std::vector<Operation> operations1{Operation{InputTypes::insert, 0, 2133},
                                           Operation{InputTypes::insert, 81, 5333},
        };

        std::vector<Operation> operations2{Operation{InputTypes::update, 0, 6754},
                                           Operation{InputTypes::update, 81, 88856},
        };

        std::vector<Operation> operations3{Operation{InputTypes::insert, 11, 7554},
                                           Operation{InputTypes::insert, 88, 45646},
        };


        const std::shared_ptr<Transaction> &transaction1 = std::make_shared<Transaction>(operations1, 0);
        const std::shared_ptr<Transaction> &transaction2 = std::make_shared<Transaction>(operations2, 1);
        const std::shared_ptr<Transaction> &transaction3 = std::make_shared<Transaction>(operations3, 2);
        transactions.push_back(transaction1);
        transactions.push_back(transaction2);
        transactions.push_back(transaction3);

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, transactions.size() / batchSize + 1);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
                Utils::initTimestampToTransactionState(transactions.size());

        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        LogWriter logWriter{logTransactions};
        logWriter.writeLog(transactions);


        std::vector<RecordsMapPtr>
                recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(ccThreadsNumber);

        Record expectedRecord1 = createUpdatedRecord(2133, 6754, transaction1, transaction2);
        Record expectedRecord2 = createUpdatedRecord(5333, 88856, transaction1, transaction2);
        Record expectedRecord3{2, LONG_MAX, *transaction3, 7554, nullptr};
        Record expectedRecord4{2, LONG_MAX, *transaction3, 45646, nullptr};


        std::vector<std::thread> ccThreads = Utils::startCCThreads(
                latches, logTransactions, recordsPartitionedByCct,
                ccThreadsNumber, batchSize, transactions.size());

        std::vector<std::thread> eThreads = Utils::startEThreads(
                recordsPartitionedByCct, logTransactions, timestampToTransactionState,
                latches, eThreadsNumber, ccThreadsNumber, batchSize
        );

        for (auto &th : ccThreads) {
            th.join();
        }

        for (auto &th : eThreads) {
            th.join();
        }

        EXPECT_EQ(Utils::getRecordsSize(recordsPartitionedByCct), 4);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 0, ccThreadsNumber), expectedRecord1);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 81, ccThreadsNumber), expectedRecord2);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 11, ccThreadsNumber), expectedRecord3);
        EXPECT_EQ(Utils::getRecord(recordsPartitionedByCct, 88, ccThreadsNumber), expectedRecord4);
    }
}  // namespace
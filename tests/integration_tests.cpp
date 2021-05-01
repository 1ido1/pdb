//
// Created by bayda on 17/04/2021.
//

#include "gtest/gtest.h"
#include <iostream>
#include <vector>
#include <memory>
#include "../input.h"
#include "../log_writer.h"
#include "../concurrency_control.h"
#include "../execution.h"
#include "../utils.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <boost/thread/latch.hpp>

namespace {
    TEST(IntegrationTest, Test) {
        int ccThreadsNumber = 3;
        int eThreadsNumber = 2;
        int batchSize = 2;
        std::vector<std::shared_ptr<Transaction>> transactions{};
        std::vector<Operation> operations{Operation{InputTypes::insert, 0, 2133},
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


        transactions.push_back(std::make_shared<Transaction>(operations, 0));
        transactions.push_back(std::make_shared<Transaction>(operations2, 1));

        std::vector<std::shared_ptr<boost::latch>> latches =
                Utils::initLatches(ccThreadsNumber, transactions.size() / batchSize);

        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
                Utils::initTimestampToTransactionState(transactions.size());

        tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
        LogWriter logWriter{logTransactions};
        logWriter.writeLog(transactions);

        tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};

        std::vector<std::thread> ccThreads= Utils::startCCThreads(
                latches, logTransactions, recordsMap,
                ccThreadsNumber, batchSize);

        std::vector<std::thread> eThreads = Utils::startEThreads(
                recordsMap, logTransactions, timestampToTransactionState,
                latches, eThreadsNumber, batchSize
                );

        for (auto &th : ccThreads) {
            th.join();
        }

        for (auto &th : eThreads) {
            th.join();
        }

        Utils::printMap<>(std::cout, recordsMap);

        EXPECT_EQ(recordsMap.size(), 10);
    }
}  // namespace
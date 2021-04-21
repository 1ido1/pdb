#ifndef TESTING

#include <iostream>
#include <vector>
#include <memory>
#include "input.h"
#include "log_writer.h"
#include "concurrency_control.h"
#include "execution.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <boost/thread/latch.hpp>

int main() {
    std::vector<std::thread> ccThreads(Constants::CC_THREADS_NUMBER);
    std::vector<std::thread> eThreads(Constants::EXECUTION_THREADS_NUMBER);

    std::vector<std::shared_ptr<Transaction>> transactions{std::vector<std::shared_ptr<Transaction>>()};
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

    std::vector<std::shared_ptr<boost::latch>> latches(transactions.size() / Constants::BATCH_SIZE);

    int ccThreadsNumber = Constants::CC_THREADS_NUMBER;
    for (int i = 0; i < latches.size(); ++i) {
        latches[i] = std::make_shared<boost::latch>(ccThreadsNumber);
    }

    tbb::concurrent_unordered_map<int, std::shared_ptr<TransactionState>> idToTransactionState;
    for (int i = 0; i < transactions.size(); ++i) {
        idToTransactionState[i] = std::make_shared<TransactionState>(TransactionState::unprocessed);
    }

    tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
    LogWriter logWriter{logTransactions};
    logWriter.writeLog(transactions);

    tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
    for (int i = 0; i < Constants::CC_THREADS_NUMBER; i++) {
        std::cout << "main() : creating cc thread, " << i << std::endl;
        ConcurrencyControl cc{recordsMap, logTransactions, latches, i};
        ccThreads[i] = std::thread(&ConcurrencyControl::readFromLog, cc);
    }

    for (int i = 0; i < Constants::EXECUTION_THREADS_NUMBER; i++) {
        std::cout << "main() : creating execution thread, " << i << std::endl;
        Execution execution{recordsMap, logTransactions, idToTransactionState, latches, i};
        eThreads[i] = std::thread(&Execution::readFromLog, execution);
    }

    for (auto &th : ccThreads) {
        th.join();
    }

    for (auto &th : eThreads) {
        th.join();
    }

    std::cout << "result ";
    Utils::printMap<>(std::cout, recordsMap);

    return 0;
}

#endif

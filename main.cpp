#ifndef TESTING

#include <iostream>
#include <vector>
#include <memory>
#include "read_input_file.h"
#include "spdlog/spdlog.h"
#include "structures/input.h"
#include "log_writer.h"
#include "concurrency_control.h"
#include "execution.h"
#include "utils.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <boost/thread/latch.hpp>


void setDebugLevel();

int main(int argc, char *argv[]) {
    std::vector<std::string> paths;
    if (argc == 1) {
        spdlog::info("No arguments passed to program");
        return 0;
    }
    
    setDebugLevel();

    for (int i = 1; i < argc; i++) {
        paths.emplace_back(argv[i]);
    }
    const std::vector<std::shared_ptr<Transaction>> &transactions = ReadInputFile::readFiles(paths, 10);

    std::vector<std::shared_ptr<boost::latch>> latches =
            Utils::initLatches(Constants::CC_THREADS_NUMBER,
                               transactions.size() / Constants::BATCH_SIZE + 1);

    tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
            Utils::initTimestampToTransactionState(transactions.size());
    std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(Constants::CC_THREADS_NUMBER);

    tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
    LogWriter logWriter{logTransactions};
    std::thread lwThread = std::thread(&LogWriter::writeLog, logWriter, transactions);

    std::vector<std::thread> ccThreads= Utils::startCCThreads(
            latches, logTransactions, recordsPartitionedByCct,
            Constants::CC_THREADS_NUMBER, Constants::BATCH_SIZE, transactions.size());

    std::vector<std::thread> eThreads = Utils::startEThreads(
            recordsPartitionedByCct, logTransactions, timestampToTransactionState,
            latches, Constants::EXECUTION_THREADS_NUMBER, Constants::CC_THREADS_NUMBER, Constants::BATCH_SIZE
    );

    // wait for execution to finish
    lwThread.join();

    for (auto &th : ccThreads) {
        th.join();
    }

    for (auto &th : eThreads) {
        th.join();
    }

//    Utils::printMap<>(std::cout, recordsMap);
    spdlog::info("transactions size {}", transactions.size());
    spdlog::info("records size {}", Utils::getRecordsSize(recordsPartitionedByCct));

    return 1;
}

void setDebugLevel() {
    const std::string &debugLevel = Constants::getEnvironmentVariableOrDefault("LOG_LEVEL", "info");
    spdlog::set_level(spdlog::level::from_str(debugLevel));
}

#endif

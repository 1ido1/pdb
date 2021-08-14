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

void executeTransactions(const std::vector<std::shared_ptr<Transaction>> &transactions,
                         std::vector<std::shared_ptr<boost::latch>> &latches,
                         tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
                         std::vector<RecordsMapPtr> &recordsPartitionedByCct,
                         tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                         unsigned long logPosition, unsigned long transactionSize);

void try1(const std::vector<std::shared_ptr<Transaction>> &loadTransactions,
          std::vector<std::shared_ptr<boost::latch>> &latches,
          tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
          std::vector<RecordsMapPtr> &recordsPartitionedByCct,
          tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions);

int main(int argc, char *argv[]) {
    std::vector<std::string> loadPath;
    std::vector<std::string> workloadPaths;
    if (argc == 1) {
        spdlog::info("No arguments passed to program");
        return 0;
    }

    setDebugLevel();

    loadPath.emplace_back(argv[1]);

    for (int i = 2; i < argc; i++) {
        workloadPaths.emplace_back(argv[i]);
    }

    const std::vector<std::shared_ptr<Transaction>> &loadTransactions = ReadInputFile::readFiles(loadPath, 10);
    const std::vector<std::shared_ptr<Transaction>> &workloadTransactions = ReadInputFile::readFiles(workloadPaths, 10);

    std::vector<std::shared_ptr<boost::latch>> latches =
            Utils::initLatches(Constants::CC_THREADS_NUMBER,
                               (loadTransactions.size() + workloadTransactions.size()) / Constants::BATCH_SIZE + 2);

    tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState =
            Utils::initTimestampToTransactionState(loadTransactions.size() + workloadTransactions.size());
    std::vector<RecordsMapPtr> recordsPartitionedByCct = Utils::initRecordsPartitionedByCct(
            Constants::CC_THREADS_NUMBER);

    tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;

    executeTransactions(loadTransactions, latches, timestampToTransactionState,
                        recordsPartitionedByCct, logTransactions, 0,
                        loadTransactions.size());

    if (argc == 2) {
        spdlog::info("Only load workload, finished");
        return 1;
    }

    executeTransactions(workloadTransactions, latches, timestampToTransactionState,
                        recordsPartitionedByCct, logTransactions, loadTransactions.size(),
                        loadTransactions.size() + workloadTransactions.size());


    return 1;
}

void executeTransactions(const std::vector<std::shared_ptr<Transaction>> &transactions,
                         std::vector<std::shared_ptr<boost::latch>> &latches,
                         tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
                         std::vector<RecordsMapPtr> &recordsPartitionedByCct,
                         tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                         unsigned long logPosition, unsigned long transactionSize) {
    LogWriter logWriter{logTransactions};
    std::thread lwThread = std::thread(&LogWriter::writeLog, logWriter, transactions);

    std::vector<std::thread> ccThreads = Utils::startCCThreads(
            latches, logTransactions, recordsPartitionedByCct,
            logPosition, Constants::CC_THREADS_NUMBER, Constants::BATCH_SIZE,
            transactionSize);

    std::vector<std::thread> eThreads = Utils::startEThreads(
            recordsPartitionedByCct, logTransactions, timestampToTransactionState,latches,
            logPosition, Constants::EXECUTION_THREADS_NUMBER, Constants::CC_THREADS_NUMBER, Constants::BATCH_SIZE
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
    spdlog::info("transactions size {}", transactionSize);
    spdlog::info("records size {}", Utils::getRecordsSize(recordsPartitionedByCct));
}

void setDebugLevel() {
    const std::string &debugLevel = Constants::getEnvironmentVariableOrDefault("LOG_LEVEL", "info");
    spdlog::set_level(spdlog::level::from_str(debugLevel));
}

#endif

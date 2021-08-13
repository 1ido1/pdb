//
// Created by bayda on 30/01/2021.
//

#ifndef PDB_UTILS_H
#define PDB_UTILS_H


#include <vector>
#include <iostream>
#include <map>
#include <memory>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <boost/thread/latch.hpp>
#include <thread>
#include "spdlog/spdlog.h"
#include "structures/state.h"
#include "structures/input.h"
#include "concurrency_control.h"
#include "execution.h"
#include "constants.h"

class Utils {
public:

    template<class T1, class T2>
    static void printMap(std::ostream &os, tbb::concurrent_unordered_map<T1, std::shared_ptr<T2>> &map) {
        for (auto elem : map) {
            os << "[" << elem.first << " " << *elem.second << "]" << ", ";
        }
        os << std::endl;
    }

    static std::vector<std::shared_ptr<boost::latch>> initLatches(int ccThreadsNumber, unsigned long latchesSize) {
        std::vector<std::shared_ptr<boost::latch>> latches(latchesSize);

        for (int i = 0; i < latchesSize; ++i) {
            latches[i] = std::make_shared<boost::latch>(ccThreadsNumber);
        }

        return latches;
    }

    static std::vector<RecordsMapPtr>
    initRecordsPartitionedByCct(const int ccThreadsNumber) {
        std::vector<RecordsMapPtr> recordsPartitionedByCct{};
        recordsPartitionedByCct.reserve(ccThreadsNumber);

        for (int i = 0; i < ccThreadsNumber; ++i) {
            recordsPartitionedByCct.push_back(
                    std::make_shared<tbb::concurrent_unordered_map<int, std::shared_ptr<Record>>>());
        }

        return recordsPartitionedByCct;
    }

    static tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>>
    initTimestampToTransactionState(unsigned long size) {
        tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> timestampToTransactionState;
        for (int i = 0; i < size; ++i) {
            timestampToTransactionState[i] = std::make_shared<TransactionState>(TransactionState::unprocessed);
        }

        return timestampToTransactionState;
    }

    static std::vector<std::thread>
    startCCThreads(std::vector<std::shared_ptr<boost::latch>> &latches,
                   tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                   std::vector<RecordsMapPtr> &recordsPartitionedByCct, int ccThreadsNumber,
                   int batchSize, unsigned long transactionSize) {
        std::vector<std::thread> ccThreads(ccThreadsNumber);
        for (int i = 0; i < ccThreadsNumber; i++) {
            spdlog::info("creating cc thread {}", i);
            ConcurrencyControl cc{recordsPartitionedByCct[i], logTransactions, latches,
                                  i, ccThreadsNumber, batchSize, transactionSize};
            ccThreads[i] = std::thread(&ConcurrencyControl::readFromLog, cc);
        }
        return ccThreads;

    }

    static std::vector<std::thread>
    startEThreads(std::vector<RecordsMapPtr>
                  &recordsPartitionedByCct,
                  tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions,
                  tbb::concurrent_unordered_map<long, std::shared_ptr<TransactionState>> &timestampToTransactionState,
                  std::vector<std::shared_ptr<boost::latch>> &latches,
                  int eThreadsNumber,
                  int ccThreadsNumber,
                  int batchSize) {
        std::vector<std::thread> eThreads(eThreadsNumber);
        for (int i = 0; i < eThreadsNumber; i++) {
            spdlog::info("creating execution thread {}", i);
            Execution execution{recordsPartitionedByCct, logTransactions, timestampToTransactionState,
                                latches, i, eThreadsNumber, ccThreadsNumber, batchSize};
            eThreads[i] = std::thread(&Execution::readFromLog, execution);
        }
        return eThreads;

    }

    static size_t getCcThreadNumber(long key, int totalCCThreads) {
        return std::hash<int>{}(key) % totalCCThreads;
    }

    static Record getRecord(
            std::vector<RecordsMapPtr> &recordsPartitionedByCct,
            long key, int totalCCThreads) {
        return *recordsPartitionedByCct.at(Utils::getCcThreadNumber(key, totalCCThreads))->at(key).get();
    }

    static int getRecordsSize(
            std::vector<RecordsMapPtr>
            &recordsPartitionedByCct) {
        int size = 0;
        for (RecordsMapPtr &map: recordsPartitionedByCct) {
            size += map->size();
        }

        return size;
    }


};

#endif //PDB_UTILS_H
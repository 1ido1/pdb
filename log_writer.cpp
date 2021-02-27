//
// Created by bayda on 17/12/2020.
//

#include "log_writer.h"

LogWriter::LogWriter(tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions)
: logTransactions(logTransactions) {}

void LogWriter::writeLog(std::vector<std::shared_ptr<Transaction>> transactions) {
    for (auto transaction : transactions) {
        logTransactions.push_back(transaction);
    }
}

//
// Created by bayda on 17/12/2020.
//

#include "log_writer.h"

LogWriter::LogWriter(tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions)
: logTransactions(logTransactions) {}

void LogWriter::writeLog(const std::vector<std::shared_ptr<Transaction>>& transactions) {
    for (const auto& transaction : transactions) {
        logTransactions.push_back(transaction);
    }
}

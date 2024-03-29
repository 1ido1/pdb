 //
// Created by bayda on 17/12/2020.
//

#ifndef PDB_LOG_WRITER_H
#define PDB_LOG_WRITER_H


#include <memory>
#include <tbb/concurrent_vector.h>
#include "structures/input.h"

 class LogWriter {
     tbb::concurrent_vector<std::shared_ptr<Transaction>>& logTransactions;
 public:
     explicit LogWriter(tbb::concurrent_vector<std::shared_ptr<Transaction>> &logTransactions);
     void writeLog(const std::vector<std::shared_ptr<Transaction>>& transactions);
 };


#endif //PDB_LOG_WRITER_H

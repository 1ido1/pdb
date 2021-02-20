 //
// Created by bayda on 17/12/2020.
//

#ifndef PDB_LOG_WRITER_H
#define PDB_LOG_WRITER_H


#include <memory>
#include "input.h"

 class LogWriter {
    std::vector<std::shared_ptr<Transaction>>& logTransactions;
 public:
     explicit LogWriter(std::vector<std::shared_ptr<Transaction>> &logTransactions);
     void writeLog(std::vector<std::shared_ptr<Transaction>> transactions);
 };


#endif //PDB_LOG_WRITER_H

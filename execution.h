
//
// Created by bayda on 12/02/2021.
//

#ifndef PDB_EXECUTION_H
#define PDB_EXECUTION_H

#include <map>
#include <memory>
#include <set>
#include "record.h"

class Execution {
private:
    std::map<int,std::shared_ptr<Record>>& recordsMap;
    const std::vector<std::shared_ptr<Transaction>>& logTransactions;
    int threadNumber;
    int batchPosition = 0;
    std::set<int> transactionsToExecute;

public:
    Execution(std::map<int, std::shared_ptr<Record>> &recordsMap,
              const std::vector<std::shared_ptr<Transaction>> &logTransactions, int threadNumber);
    void readFromLog();

};

#endif //PDB_EXECUTION_H

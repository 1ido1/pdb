//
// Created by bayda on 22/01/2021.
//

#ifndef PDB_CONCURRENCY_CONTROL_H
#define PDB_CONCURRENCY_CONTROL_H


#include <map>
#include "record.h"
#include "constants.h"

class ConcurrencyControl {
private:
    constexpr static double INITIALIZED_VALUE = -9999;
    std::map<int,std::shared_ptr<Record>>& recordsMap;
    const std::vector<std::shared_ptr<Transaction>>& logTransactions;
    int logPosition = 0;

public:
    ConcurrencyControl(std::map<int, std::shared_ptr<Record>> &recordsMap,
                       const std::vector<std::shared_ptr<Transaction>> &logTransactions);
    void writeOperation(Operation operation, const Transaction &transaction);
    void writeTransaction(const Transaction &transaction);
    void readFromLog();
};


#endif //PDB_CONCURRENCY_CONTROL_H

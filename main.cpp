
#include <iostream>
#include <vector>
#include <memory>
#include "input.h"
#include "log_writer.h"
#include "concurrency_control.h"

int main() {

    //     explicit LogWriter(const std::vector<std::shared_ptr<Transaction>> &logTransactions);
    std::vector<std::shared_ptr<Transaction>> transactions{std::vector<std::shared_ptr<Transaction>>()};
    std::vector<Operation> operations {Operation{InputTypes::insert, 0, 2133},
                                       Operation{InputTypes::insert, 4, 345345},
                                       Operation{InputTypes::insert, 3, 45345},
                                       Operation{InputTypes::insert, 77, 3421},
                                       Operation{InputTypes::insert, 81, 5333},
                                       };
    transactions.push_back(std::make_shared<Transaction>(operations, 0));

    std::vector<std::shared_ptr<Transaction>> logTransactions;
    LogWriter logWriter{logTransactions};
    logWriter.writeLog(transactions);

    std::map<int, std::shared_ptr<Record>> recordsMap{};
    ConcurrencyControl cc{recordsMap, logTransactions};
    cc.readFromLog();

    std::cout << "result ";
    Utils::printMap<>(std::cout, recordsMap);



    return 0;
}

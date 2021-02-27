
#include <iostream>
#include <vector>
#include <memory>
#include "input.h"
#include "log_writer.h"
#include "concurrency_control.h"
#include "execution.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <pthread.h>

void *PrintHello(void *threadid) {
    long tid;
    tid = (long)threadid;
    std::cout << "Hello World! Thread ID, " << tid << std::endl;
    pthread_exit(NULL);
}

int main() {
    pthread_t threads[Constants::NUM_THREADS];
//    int rc;
//    int i;
//
//    for( i = 0; i < NUM_THREADS; i++ ) {
//        std::cout << "main() : creating thread, " << i << std::endl;
//        rc = pthread_create(&threads[i], NULL, PrintHello, (void *)i);
//
//        if (rc) {
//            std::cout << "Error:unable to create thread," << rc << std::endl;
//            exit(-1);
//        }
//    }
//    pthread_exit(NULL);

    tbb::concurrent_unordered_map<int, int> ht;
    tbb::concurrent_vector<int> bla;

    std::vector<std::shared_ptr<Transaction>> transactions{std::vector<std::shared_ptr<Transaction>>()};
    std::vector<Operation> operations {Operation{InputTypes::insert, 0, 2133},
                                       Operation{InputTypes::insert, 4, 345345},
                                       Operation{InputTypes::insert, 3, 45345},
                                       Operation{InputTypes::insert, 77, 3421},
                                       Operation{InputTypes::insert, 81, 5333},
                                       };
    transactions.push_back(std::make_shared<Transaction>(operations, 0));

    tbb::concurrent_unordered_map<int, std::shared_ptr<TransactionState>> idToTransactionState;
    idToTransactionState[0] = std::make_shared<TransactionState>(TransactionState::unprocessed);

    tbb::concurrent_vector<std::shared_ptr<Transaction>> logTransactions;
    LogWriter logWriter{logTransactions};
    logWriter.writeLog(transactions);

    tbb::concurrent_unordered_map<int, std::shared_ptr<Record>> recordsMap{};
    
    ConcurrencyControl cc{recordsMap, logTransactions, 0};
    cc.readFromLog();

    Execution execution{recordsMap, logTransactions, idToTransactionState, 0};
    execution.readFromLog();

    std::cout << "result ";
    Utils::printMap<>(std::cout, recordsMap);

    return 0;
}

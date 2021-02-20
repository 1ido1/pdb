//
// Created by bayda on 27/12/2020.
//

#ifndef PDB_INPUT_H
#define PDB_INPUT_H

#include "input_types.h"
#include "utils.h"
#include <utility>
#include <vector>
#include <ostream>

struct Operation {
    InputTypes inputType;
    int key;
    double value;
    int keyRange;

    Operation(InputTypes inputType, int key, double value, int keyRange) : inputType(inputType), key(key), value(value),
                                                                           keyRange(keyRange) {}
    Operation(InputTypes inputType, int key, double value) : inputType(inputType), key(key), value(value) {}
    Operation(InputTypes inputType, int key) : inputType(inputType), key(key) {}


    friend std::ostream &operator<<(std::ostream &os, const Operation &operation) {
        os << "inputType: ";
        switch (operation.inputType) {
            case InputTypes::insert:
                os << "insert ";
                break;
            case InputTypes::update:
                os << "update ";
                break;
            case InputTypes::modify:
                os << "modify ";
                break;
            case InputTypes::read:
                os << "read ";
                break;
            case InputTypes::scan:
                os << "scan ";
                break;
        }
        os << " key: " << operation.key << " value: " << operation.value
           << " keyRange: " << operation.keyRange;
        return os;
    }
};

struct Transaction {
    std::vector<Operation> operations;
    long timestamp;

    friend std::ostream &operator<<(std::ostream &os, const Transaction &transaction) {
        os << "operations: ";
        Utils::printVector(os, transaction.operations);
        os << " timestamp: " << transaction.timestamp;
        return os;
    }

    Transaction(std::vector<Operation> operations, long timestamp) : operations(std::move(operations)),
                                                                     timestamp(timestamp) {}
};

#endif //PDB_INPUT_H

//
// Created by bayda on 27/12/2020.
//

#ifndef PDB_INPUT_H
#define PDB_INPUT_H

#include "input_types.h"
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

    bool operator==(const Operation &rhs) const {
        return inputType == rhs.inputType &&
               key == rhs.key &&
               value == rhs.value &&
               keyRange == rhs.keyRange;
    }

    bool operator!=(const Operation &rhs) const {
        return !(rhs == *this);
    }
};

struct Transaction {
    std::vector<Operation> operations;
    long timestamp;

    friend std::ostream &operator<<(std::ostream &os, const Transaction &transaction) {
        os << "operations: ";
        printVector(os, transaction.operations);
        os << " timestamp: " << transaction.timestamp;
        return os;
    }

    Transaction(std::vector<Operation> operations, long timestamp) : operations(std::move(operations)),
                                                                     timestamp(timestamp) {}

    bool operator==(const Transaction &rhs) const {
        return std::equal(operations.begin(), operations.end(), rhs.operations.begin()) &&
               timestamp == rhs.timestamp;
    }

    bool operator!=(const Transaction &rhs) const {
        return !(rhs == *this);
    }

private:
    template<class T>
    static void printVector(std::ostream &os, std::vector<T> vec) {
        for (auto i : vec) {
            os << i << " ";
        }
        os << std::endl;
    }

};

#endif //PDB_INPUT_H

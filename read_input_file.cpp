//
// Created by bayda on 13/02/2021.
//

#include <sstream>
#include "read_input_file.h"


std::vector<std::shared_ptr<Transaction>> ReadInputFile::readFile(
        const std::string &path, int transactionSize) {
    return readFile(path, transactionSize, 0);
}

std::vector<std::shared_ptr<Transaction>> ReadInputFile::readFile(
        const std::string &path, int transactionSize, long timestamp) {
    std::string line;
    std::ifstream MyReadFile(path);
    std::vector<Operation> operations;
    std::vector<std::shared_ptr<Transaction>> transactions{std::vector<std::shared_ptr<Transaction>>()};


    while (getline(MyReadFile, line)) {
        operations.push_back(parseLine(line));

        if (operations.size() == transactionSize) {
            transactions.push_back(std::make_shared<Transaction>(operations, timestamp++));
            operations = std::vector<Operation>{};
        }
    }

    if (!operations.empty()) {
        transactions.push_back(std::make_shared<Transaction>(operations, timestamp++));
    }

    MyReadFile.close();
    return transactions;
}

Operation ReadInputFile::parseLine(const std::string &line) {
    static const std::map<std::string, InputTypes> inputTypesMap =
            boost::assign::map_list_of("READ", InputTypes::read)
                    ("UPDATE", InputTypes::update)
                    ("INSERT", InputTypes::insert)
                    ("MODIFY", InputTypes::modify)
                    ("SCAN", InputTypes::scan);

    std::istringstream stream(line);
    std::string word;

    stream >> word;

    InputTypes operationType = inputTypesMap.at(word);

    switch (operationType) {
        case InputTypes::insert:
        case InputTypes::update:
            return buildInsertOrUpdateOperation(operationType, stream);
        case InputTypes::read:
            return buildReadOperation(stream);
        case InputTypes::scan:
            return buildScanOperation(stream);
        case InputTypes::modify:
            return buildModifyOperation(stream);
    }

    return Operation(InputTypes::update, 0, 0, 0);
}

Operation ReadInputFile::buildReadOperation(std::istringstream &stream) {
    int key;
    stream >> key;

    return Operation(InputTypes::read, key);
}

Operation ReadInputFile::buildInsertOrUpdateOperation(InputTypes inputTypes, std::istringstream &stream) {
    int key;
    stream >> key;
    double value = key;

    return Operation(inputTypes, key, value);
}

Operation ReadInputFile::buildModifyOperation(std::istringstream &stream) {
    int key;
    double value;
    stream >> key;
    stream >> value;

    return Operation(InputTypes::modify, key, value);
}

Operation ReadInputFile::buildScanOperation(std::istringstream &stream) {
    int key;
    int range;
    stream >> key;
    stream >> range;

    return Operation(InputTypes::scan, key, 0, range);
}

std::vector<std::shared_ptr<Transaction>> ReadInputFile::readFiles(
        const std::vector<std::string> &paths, int transactionSize) {
    std::vector<std::shared_ptr<Transaction>> transactions{std::vector<std::shared_ptr<Transaction>>()};

    for (const std::string& path: paths) {
        const std::vector<std::shared_ptr<Transaction>> &transactionsToAdd =
                readFile(path, transactionSize, transactions.size());
        transactions.insert(transactions.end(), transactionsToAdd.begin(), transactionsToAdd.end());
    }

    return transactions;
}

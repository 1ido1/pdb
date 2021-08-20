//
// Created by bayda on 21/05/2021.
//

#ifndef PDB_READ_INPUT_FILE_H
#define PDB_READ_INPUT_FILE_H

#include <memory>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <boost/assign/list_of.hpp>
#include "structures/record.h"
#include "structures/input_types.h"

class ReadInputFile {
public:
    static std::vector<std::shared_ptr<Transaction>> readFile(const std::string &path, int transactionSize);

    static std::vector<std::shared_ptr<Transaction>> readFiles(
            const std::vector<std::string> &paths, int transactionSize);

    static std::vector<std::shared_ptr<Transaction>> readFiles(
            const std::vector<std::string> &paths, int transactionSize, long startTimestamp);

private:
    static Operation parseLine(const std::string &line);

    static Operation buildReadOperation(std::istringstream &stream);

    static Operation buildInsertOrUpdateOperation(InputTypes inputTypes, std::istringstream &stream);

    static Operation buildModifyOperation(std::istringstream &stream);

    static Operation buildScanOperation(std::istringstream &stream);

    static std::vector<std::shared_ptr<Transaction>> readFile(
            const std::string &path, int transactionSize, long timestamp);

};

#endif //PDB_READ_INPUT_FILE_H

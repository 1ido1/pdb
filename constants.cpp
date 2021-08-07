//
// Created by bayda on 07/08/2021.
//


#include "constants.h"
#include "spdlog/spdlog.h"

std::string Constants::getEnvironmentVariableOrDefault(const std::string &variable_name,
                                                       const std::string &default_value) {
    const char *value = getenv(variable_name.c_str());
    const std::string &output = value ? value : default_value;
    spdlog::info("variable {} with value {}", variable_name, output);
    return output;
}

int Constants::getIntEnvironmentVariableOrDefault(const std::string &variable_name,
                                                  int default_value) {
    const char *value = getenv(variable_name.c_str());
    int output = value ? std::stoi(value) : default_value;
    spdlog::info("variable {} with value {}", variable_name, output);
    return output;
}

const int Constants::BATCH_SIZE = getIntEnvironmentVariableOrDefault("BATCH_SIZE", 3);
const int Constants::EXECUTION_THREADS_NUMBER = getIntEnvironmentVariableOrDefault("EXECUTION_THREADS_NUMBER", 2);
const int Constants::CC_THREADS_NUMBER = getIntEnvironmentVariableOrDefault("CC_THREADS_NUMBER", 3);
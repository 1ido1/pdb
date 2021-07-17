//
// Created by bayda on 21/05/2021.
//

#include "gtest/gtest.h"
#include "../read_input_file.h"


namespace {
#define EXPECT_EQ_PTR_VEC(t1, t2) \
    EXPECT_EQ(t1.size(), t2.size());  \
    for (int i = 0; i < t1.size(); ++i) { \
        EXPECT_EQ(*t1[i],*t2[i]); \
    }


    TEST(ReadInputFileTests, TransactionSize5) {
        std::vector<std::shared_ptr<Transaction>> expectedTransactions{std::vector<std::shared_ptr<Transaction>>()};
        std::vector<Operation> operations{Operation{InputTypes::read, 980,},
                                          Operation{InputTypes::update, 144, 144},
                                          Operation{InputTypes::insert, 985, 985},
                                          Operation{InputTypes::scan, 31775570, 0, 11},
                                          Operation{InputTypes::modify, 324, 2134}};


        expectedTransactions.push_back(std::make_shared<Transaction>(operations, 0));

        const std::vector<std::shared_ptr<Transaction>> &output = ReadInputFile::readFile(
                "../resources/example", 5);

        EXPECT_EQ_PTR_VEC(output, expectedTransactions);
    }

    TEST(ReadInputFileTests, TransactionSize2) {
        std::vector<std::shared_ptr<Transaction>> expectedTransactions{std::vector<std::shared_ptr<Transaction>>()};
        std::vector<Operation> operations1{Operation{InputTypes::read, 980,},
                                           Operation{InputTypes::update, 144, 144}
        };

        std::vector<Operation> operations2{Operation{InputTypes::insert, 985, 985},
                                           Operation{InputTypes::scan, 31775570, 0, 11}
        };

        std::vector<Operation> operations3{Operation{InputTypes::modify, 324, 2134}};


        expectedTransactions.push_back(std::make_shared<Transaction>(operations1, 0));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations2, 1));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations3, 2));


        const std::vector<std::shared_ptr<Transaction>> &output = ReadInputFile::readFile(
                "../resources/example", 2);

        EXPECT_EQ_PTR_VEC(output, expectedTransactions);
    }

    TEST(ReadInputFileTests, TransactionSize3) {
        std::vector<std::shared_ptr<Transaction>> expectedTransactions{std::vector<std::shared_ptr<Transaction>>()};
        std::vector<Operation> operations1{Operation{InputTypes::read, 980,},
                                           Operation{InputTypes::update, 144, 144},
                                           Operation{InputTypes::insert, 985, 985}
        };

        std::vector<Operation> operations2{Operation{InputTypes::scan, 31775570, 0, 11},
                                           Operation{InputTypes::modify, 324, 2134}
        };


        expectedTransactions.push_back(std::make_shared<Transaction>(operations1, 0));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations2, 1));

        const std::vector<std::shared_ptr<Transaction>> &output = ReadInputFile::readFile(
                "../resources/example", 3);

        EXPECT_EQ_PTR_VEC(output, expectedTransactions);
    }

    TEST(ReadInputFileTests, twoFiles) {
        std::vector<std::shared_ptr<Transaction>> expectedTransactions{std::vector<std::shared_ptr<Transaction>>()};
        std::vector<Operation> operations1{Operation{InputTypes::read, 980,},
                                           Operation{InputTypes::update, 144, 144}
        };

        std::vector<Operation> operations2{Operation{InputTypes::insert, 985, 985},
                                           Operation{InputTypes::scan, 31775570, 0, 11}
        };

        std::vector<Operation> operations3{Operation{InputTypes::modify, 324, 2134}
        };

        std::vector<Operation> operations4{Operation{InputTypes::read, 765,},
                                           Operation{InputTypes::update, 855, 855}
        };

        std::vector<Operation> operations5{Operation{InputTypes::insert, 333, 333},
                                           Operation{InputTypes::scan, 8888, 0, 4}
        };

        std::vector<Operation> operations6{Operation{InputTypes::modify, 456, 877}
        };


        expectedTransactions.push_back(std::make_shared<Transaction>(operations1, 0));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations2, 1));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations3, 2));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations4, 3));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations5, 4));
        expectedTransactions.push_back(std::make_shared<Transaction>(operations6, 5));


        std::vector<std::string> paths{"../resources/example",
                                       "../resources/example2"};
        const std::vector<std::shared_ptr<Transaction>> &output = ReadInputFile::readFiles(
                paths, 2);

        EXPECT_EQ_PTR_VEC(output, expectedTransactions);
    }
}
//
// Created by bayda on 22/01/2021.
//

#ifndef PDB_RECORD_H
#define PDB_RECORD_H


#include <ostream>
#include "input.h"

struct Record {
    long beginTimestamp;
    long endTimestamp;
    const Transaction &transaction;
    double value;
    std::shared_ptr<Record> prev;

public:
    Record(long beginTimestamp, long endTimestamp,
           const Transaction &transaction, double value, std::shared_ptr<Record> prev)
            : beginTimestamp(beginTimestamp), endTimestamp(endTimestamp), transaction(transaction), value(value),
              prev(prev) {}

    friend std::ostream &operator<<(std::ostream &os, const Record &record) {
        os << "(beginTimestamp: " << record.beginTimestamp << ", endTimestamp: " << record.endTimestamp
           << ", transaction timestamp: " << record.transaction.timestamp << ", value: " << record.value << ", prev: "
           << record.prev << ")";
        return os;
    }

    bool operator==(const Record &rhs) const {
        return beginTimestamp == rhs.beginTimestamp &&
               endTimestamp == rhs.endTimestamp &&
               transaction == rhs.transaction &&
               value == rhs.value &&
                (prev == nullptr && rhs.prev == nullptr ||
               *prev == *rhs.prev);
    }

    bool operator!=(const Record &rhs) const {
        return !(rhs == *this);
    }
};


#endif //PDB_RECORD_H

//
// Created by bayda on 30/01/2021.
//

#ifndef PDB_UTILS_H
#define PDB_UTILS_H


#include <vector>
#include <iostream>
#include <map>
#include <memory>
#include <tbb/concurrent_unordered_map.h>

class Utils {
public:
    template<class T>
    static void printVector(std::ostream &os, std::vector<T> vec) {
        for(auto i : vec) {
            os << i << " ";
        }
        os << std::endl;
    }
    template<class T1,class T2>
    static void printMap(std::ostream &os, tbb::concurrent_unordered_map<T1,std::shared_ptr<T2>>& map) {
        for(auto elem : map)
        {
            os << "[" << elem.first << " " << *elem.second << "]" << ", ";
        }
        os << std::endl;
    }
};


#endif //PDB_UTILS_H

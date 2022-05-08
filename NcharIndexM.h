#pragma once

#include <metall/metall.hpp>

#include <metall/container/vector.hpp>
#include <metall/container/map.hpp>
#include <metall/container/string.hpp>
#include <unordered_set>
#include <string>
#include <vector>

typedef unsigned int ID;

using metall::container::vector;
using metall::container::map;
using metall::container::string;

class NcharIndexM {

protected:
    metall::manager man;

    map<string,vector<ID>> *db;

public:
    // const size_t RAM_MB_LIMIT = 2000; // Use at most 2GB RAM as buffer
    const int N;
    const int NUM_HASH_BUCKETS;
    NcharIndexM(int N, const char *db_dir, int NUM_HASH_BUCKETS=0);
    std::unordered_set<std::string> text2NChars(const std::string &text) const; //helper function to convert text to nchars
    // size_t batchAddFromCSVFile(std::string intermediate_filename); //the intermediate file should contain multiple lines of id,text
    void addText(ID id, std::string text); //add a single id,text pair into index
    void compactDB(); //remove duplicate IDs,and use shrink_to_fit to release unused disk space
};

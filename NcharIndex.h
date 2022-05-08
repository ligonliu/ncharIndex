#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_set>
#include <map>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

using namespace std;
typedef unsigned int ID;


class NcharIndex {

protected:
    rocksdb::DB *db;

    vector<ID> readDb(string nchar);
    void writeDb(string nchar, const vector<ID> &ids);
    vector<ID> appendDb(string nchar, const vector<ID> &new_ids);

    unordered_map<string, vector<ID> > batch_buffer;

    unordered_set<string> getIndexedSuperStrings(string text);

public:
    const size_t RAM_MB_LIMIT = 2000; // Use at most 2GB RAM as buffer
    const int N;
    const int NUM_HASH_BUCKETS;
    NcharIndex(int N, string db_dir, int NUM_HASH_BUCKETS=0);
    unordered_set<string> text2NChars(const string &text) const; //helper function to convert text to nchars
    size_t batchAddFromCSVFile(string intermediate_filename); //the intermediate file should contain multiple lines of id,text
    void addText(ID id, string text); //add a single id,text pair into index
    void removeText(ID id, string text); //remove id,text from index
    void removeId(ID id); //remove id from index (slow)
    vector<ID> lookup(string text); //look up for possible ids of text that matches
    static size_t estimateBufferSize(const unordered_map<string, vector<ID> > &buffer);
    void addToBatch(ID id, string text); //add a single id,text pair into batch
    void commitBatch(); //write batch into DB
    size_t estimateBatchBufferSize() const;
    void compactDB();
};

#include "NcharIndex.h"
#include <fstream>
#include <iostream>
#include "hps/hps.h"

using namespace std;

void mergeIntoIDset(vector<ID> &ids1, const vector<ID> &ids2)
{
    ids1.insert(ids1.end(),ids2.begin(),ids2.end());
    sort(ids1.begin(),ids1.end());
    ids1.resize(unique(ids1.begin(),ids1.end())-ids1.begin());
}

vector<ID> NcharIndex::readDb(string nchar)
{
    vector<ID> ids;
    string ids_str;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), nchar, &ids_str);
    if (s.IsNotFound()){
        throw(out_of_range(nchar));
    }
    assert(s.ok());
    hps::from_string(ids_str, ids);
    return ids;
}

void NcharIndex::writeDb(string nchar, const vector<ID> &ids)
{
    string ids_str = hps::to_string(ids);
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), nchar, ids_str);
    assert(s.ok());
}

vector<ID> NcharIndex::appendDb(string nchar, const vector<ID> &new_ids) {
    bool found=false;
    string existing;
    vector<ID> ids;
    db->KeyMayExist(rocksdb::ReadOptions(), nchar, &existing, &found);
    if (found) {
        hps::from_string(existing, ids);
        //concatenate new_ids to old_ids
        ids.insert(ids.end(), new_ids.begin(), new_ids.end());
    }
    else{
        ids = new_ids;
    }

    // sort and deduplicate
    sort(ids.begin(),ids.end());
    ids.resize(std::unique(ids.begin(), ids.end())-ids.begin());

    writeDb(nchar, ids);
    return ids;
}

NcharIndex::NcharIndex(int N, string db_dir, int NUM_HASH_BUCKETS): N(N), NUM_HASH_BUCKETS(NUM_HASH_BUCKETS){
    rocksdb::Options options;
    options.create_if_missing = true;
    auto status = rocksdb::DB::Open(options, db_dir, &db);
    assert(status.ok());
};

size_t NcharIndex::estimateBufferSize(const unordered_map<string, vector<ID> > &buffer)
{
    size_t s=0;
    for(auto &kv:buffer){
        s+=kv.first.length();
        s+=kv.second.capacity()*sizeof(ID);
    }
    return s;
}

size_t NcharIndex::estimateBatchBufferSize() const
{
    return NcharIndex::estimateBufferSize(batch_buffer);
}

unordered_set<string> NcharIndex::text2NChars(const string &text) const
{
    unordered_set<string> nchars;
    if (text.length()==0){
        return nchars;
    }

    if(NUM_HASH_BUCKETS==0){
        auto len = min((size_t)N,text.length());
        for(auto begin=0;begin<=text.length()-len;begin++){
            nchars.insert(text.substr(begin, len));
        }
    }
    else{
        string hash_text = text;
        for(auto &hchar:hash_text){
            hchar %= NUM_HASH_BUCKETS;
        }
        auto len = min((size_t)N,text.length());
        for(int begin=0;begin<=hash_text.length()-len;begin++){
            nchars.insert(hash_text.substr(begin, len));
        }
    }
    return nchars;
}

size_t NcharIndex::batchAddFromCSVFile(string intermediate_filename)
{
    //unordered_map<string, vector<ID> > buffer;
    size_t count_line = 0;
    string line;
    ifstream is(intermediate_filename.c_str());

    cout<<"Progress: "<<endl;

    while(!is.eof()){
        getline(is, line);
        if (line.size()<3) continue;
        auto comma_index = line.find(',');
        if(comma_index>=line.size()-1) continue;

        auto id_str = line.substr(0,comma_index);
        ID id = stoull(id_str);

        auto nchars = text2NChars(line.substr(comma_index + 1));
        for(auto &nchar:nchars){
            batch_buffer[nchar].push_back(id);
        }
        count_line++;

        if(count_line%10000==0){
            cout<<count_line/1000<< "K"<<endl;
            auto buffer_size = estimateBufferSize(batch_buffer);
            if (buffer_size>RAM_MB_LIMIT*1048576) {
                //empty every vector into DB
                cout<<"Syncing..."<<endl;
                for (auto &kv:batch_buffer){
                    if (kv.second.size()>0){
                        appendDb(kv.first, kv.second);
                        vector<ID> empty;
                        kv.second.swap(empty);
                    }
                }
            }
        }
    }
    //after the file is finished, we read again
    cout<<"Syncing..."<<endl;
    for (auto &kv:batch_buffer){
        if (kv.second.size()>0){
            appendDb(kv.first, kv.second);
            vector<ID> empty;
            kv.second.swap(empty);
        }
    }
    compactDB();
    return count_line;
}

void NcharIndex::addText(ID id, string text) {
    auto nchars = text2NChars(text);
    vector<ID> single_id = {id};
    for(auto nchar:nchars){
        appendDb(nchar, single_id);
    }
}

void NcharIndex::addToBatch(ID id, string text)
{
    auto nchars = text2NChars(text);
    for(auto nchar:nchars){
        batch_buffer[nchar].push_back(id);
    }
}

void NcharIndex::commitBatch()
{
    rocksdb::WriteBatch wb;

    for (auto &kv:batch_buffer){
        if (kv.second.size()>0){
            vector<ID> newids;
            try {
                newids = readDb(kv.first);
            }
            catch(out_of_range &e){
                // do nothing, since it means there is no old value
            }

            //merge kv.second into oldvalue
            mergeIntoIDset(newids,kv.second);

            wb.Put(kv.first,hps::to_string(newids));

            vector<ID> empty;
            kv.second.swap(empty); //this is a trick to release vector memory without releasing the vector object
        }
    }
    batch_buffer.clear();
    auto wo = rocksdb::WriteOptions();
    wo.sync = true;
    auto s = db->Write(wo, &wb);
    assert(s.ok());
}

void NcharIndex::compactDB() {
    db->CompactRange(rocksdb::CompactRangeOptions(),nullptr,nullptr);
}

void NcharIndex::removeText(ID id, string text)
{
    auto nchars = text2NChars(text);
    for(auto nchar:nchars){
        auto ids = readDb(nchar);
        ids.erase(remove(ids.begin(), ids.end(), id),  ids.end());
        writeDb(nchar,ids);
    }
}

void NcharIndex::removeId(ID id)
{
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto key = it->key().ToString();
        auto value = it->value().ToString();
        vector<ID> ids;
        hps::from_string(value, ids);
        ids.erase(remove(ids.begin(), ids.end(), id),  ids.end());
        writeDb(key,ids);
    }
}

vector<ID> NcharIndex::lookup(string text)
{
    /*
     * If text length is less than N, query the text itself, plus any superstring of text in the index
     * else, query the text2NChars of the text
     * */

    vector<ID> possible;
    vector<vector<ID> > idsets;

    unordered_set<string> nchars;

    if (text.length()>=N){
        nchars = text2NChars(text);
    }
    else{
        nchars = getIndexedSuperStrings(text);
    }


    for (auto &nchar:nchars){
        auto ids = readDb(nchar);
        sort(ids.begin(),ids.end());
        idsets.push_back(ids);
    }
    if(idsets.empty()){
        return possible;
    }
    else{
        if (text.length()>=N) {
            possible = idsets[0];
            for (auto i = 1; i < idsets.size(); i++) {
                vector<ID> intersect(idsets[i].size());
                auto end = set_intersection(idsets[i].begin(), idsets[i].end(), possible.begin(), possible.end(),
                                            intersect.begin());
                intersect.resize(end - intersect.begin());
                possible = intersect;
            }
        }
        else{
            //possible should be the union of all idsets
            unordered_set<ID> possible_set;
            for (auto &idset:idsets) {
                possible_set.insert(idset.begin(),idset.end());
            }
            possible.assign(possible_set.begin(),possible_set.end());
            sort(possible.begin(),possible.end());
        }
    }
    return possible;
}

unordered_set<string> NcharIndex::getIndexedSuperStrings(string text) {
    assert(text.length()<N);
    unordered_set<string> result;
    //iterate through the database backend
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto key = it->key().ToString();
        if (key.find(text)!=-1){
            result.insert(key);
        }
        //it->value().ToString();
    }
    delete it;
    return result;
}


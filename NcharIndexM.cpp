#include "NcharIndexM.h"


std::unordered_set<std::string> NcharIndexM::text2NChars(const std::string &text) const
{
    std::unordered_set<std::string> nchars;
    if (text.length()==0){
        return nchars;
    }

    if(NUM_HASH_BUCKETS==0){
        auto len = std::min((size_t)N,text.length());
        for(auto begin=0;begin<=text.length()-len;begin++){
            nchars.insert(text.substr(begin, len));
        }
    }
    else{
        std::string hash_text = text;
        for(auto &hchar:hash_text){
            hchar %= NUM_HASH_BUCKETS;
        }
        auto len = std::min((size_t)N,text.length());
        for(int begin=0;begin<=hash_text.length()-len;begin++){
            nchars.insert(hash_text.substr(begin, len));
        }
    }
    return nchars;
}

void NcharIndexM::addText(ID id, std::string text) {
    auto nchars = text2NChars(text);
    for(const auto &nchar:nchars){
        string nchar_m(nchar, man.get_allocator());
        auto it=db->find(nchar_m);
        if (it==db->end()){
            vector<ID> v(1,man.get_allocator());
            v[0]=id;
            db->insert(std::make_pair(nchar_m,v));
        }
        else{
            it->second.push_back(id);
        }
    }
}

void NcharIndexM::compactDB()
{
    for(auto it=db->begin();it!=db->end();it++)
    {
        it->second.shrink_to_fit();
    }
}

NcharIndexM::NcharIndexM(int N, const char *db_dir, int NUM_HASH_BUCKETS): N(N), NUM_HASH_BUCKETS(NUM_HASH_BUCKETS), man(metall::create_only, db_dir){

    db = man.construct<map<string,vector<ID> > >("db")(man.get_allocator());

}


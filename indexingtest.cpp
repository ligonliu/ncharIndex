#include <iostream>
#include <cstdlib>
#include <fstream>
#include <chrono>
#include <filesystem>
#include "NcharIndex.h"


using namespace std;
using namespace std::chrono;
/*
 * The goal of this program is testing the time to build index, and the space used in relative to text length and number of rows
 *
 * */

std::string gen_random(size_t len) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (auto i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp_s;
}


long clock()
{
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

size_t getDirectorySize(string dir)
{
    char outf[L_tmpnam], line[1000];
    tmpnam(outf);
    auto cmd = "du -sb " +  dir +" 1>" +outf + " 2>/dev/null";
    system(cmd.c_str());
    ifstream ifs(outf);
    ifs.getline(line,1000);
    ifs.close();
    return strtol(line, nullptr,10);
}

vector<size_t> testIndexing(size_t text_len, ID n_rows, unsigned int N)
{
    auto temp_dir = std::filesystem::temp_directory_path();
    auto db_dir = temp_dir.string() + "/idxtestdb";
    auto rm_cmd = "rm -rf " + db_dir + "/*";
    system(rm_cmd.c_str());

    NcharIndex idx(N,db_dir);

    //generate random strings and store in a file
    ofstream textf(temp_dir.string()+"/idxtext");

    auto total_text_size = sizeof(char)*n_rows*text_len;

    for(auto i=0;i<n_rows;i++){
        textf <<  gen_random(text_len);
    }
    textf.close();

    auto begin = clock();
    ifstream textif(temp_dir.string()+"/idxtext");
    vector<char> buffer(text_len+1,'\0');

    for(auto i=0;i<n_rows;i++){
        textif.read(&buffer[0],text_len);
        idx.addToBatch(i,&buffer[0]);

        if (i % (10000000/text_len)==0){
            idx.commitBatch();
        }
    }
    textif.close();
    idx.commitBatch();
    idx.compactDB();
    auto end = clock();

    auto index_dir_size = getDirectorySize(db_dir);

    vector<size_t> res = {text_len,n_rows,N,static_cast<unsigned long>(end-begin),index_dir_size,total_text_size};

    return res;
}

int main(int argc, char ** argv)
{
    if(argc<4){
        cout<<"Usage: indexingtest text_len n_rows N"<<endl;
        cout<<"outputs: text_len n_rows N time_in_ms index_size text_size"<<endl;
        exit(0);
    }
    auto text_len = atoi(argv[1]), n_rows = atoi(argv[2]), N=atoi(argv[3]);
    auto res = testIndexing(text_len,n_rows,N);
    for (auto v:res){
        cout<<v<<' ';
    }
    cout<<endl;
    return 0;
}
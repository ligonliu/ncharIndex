#include <iostream>
#include <cstdlib>
#include <fstream>
#include <chrono>
#include <getopt.h>
#include <bits/getopt_ext.h>

#include <mysql-cppconn-8/mysqlx/xdevapi.h>
#include <boost/algorithm/string.hpp>
#include "NcharIndex.h"

using namespace std;
using namespace std::chrono;
using namespace mysqlx;
using namespace boost;

// compare join times by:
// 1. using SQL like
// 2. using index: "first get a group of ids, then search within this group of id"

vector<std::string> split(const std::string &s, char delim) {
    vector<std::string> result;
    stringstream ss (s);
    std::string item;

    while (getline (ss, item, delim)) {
        result.push_back (item);
    }

    return result;
}


vector<std::string> gettop1000frequentwords(std::string dir)
{
    auto path = dir+"/words_219k.txt";
    ifstream ifs;
    ifs.open(path);
    char line[10000];

    vector<std::string> res;

    while(!ifs.eof() and res.size()<1100){
        ifs.getline(line,10000);
        if (not isdigit(line[0])) continue;
        auto line_split = split(line,'\t');
        res.push_back(line_split[1]);
    }
    return res;
}

void print_usage()
{
    cout<<"Usage: queryIndex --index-dir index_dir --mysql-url mysqlx://user:password@server:port/ --schema schema_name --table table_name --column column_name"<<endl;

}

static struct option long_options[] = {
        {"index-dir", required_argument, NULL, 'i'},
        {"mysql-url", required_argument, NULL, 'm'},
        {"schema", required_argument, NULL, 's'},
        {"table", required_argument, NULL, 't'},
        {"column", required_argument, NULL, 'c'},
        {NULL, 0, NULL, 0}
};

long clock()
{
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

long timeQueryingDatabase(Session &sess, std::string query, long &count)
{
    auto begin = clock();
    auto row = sess.sql(query).execute().fetchOne();
    auto end = clock();
    count = row[0].get<long>();
    cout<< "count: " << count <<endl;
    return end-begin;
}

std::string toInsertStatement(const vector<ID> &index_query_result)
{
    if (!index_query_result.empty()) {
        std::stringstream query_fill_index_with_query_result;
        query_fill_index_with_query_result << "insert into index_query_result values ";

        for(auto i=0;i<index_query_result.size()-1;i++){
            query_fill_index_with_query_result << '('<<index_query_result[i]<<')'<<',';
        }
        query_fill_index_with_query_result<<'('<<index_query_result[index_query_result.size()-1]<<')';

        return query_fill_index_with_query_result.str();
    }

    return "";
}

long timeQueryingIndex(Session &sess, std::string new_query, NcharIndex &index, std::string text, vector<ID> &result)
{
    auto begin = clock();
    result = index.lookup(text);
    //cout<<"Found count in index: "<<result.size()<<endl;

    if(text.length()>=index.N) {
        auto insert_stmt = toInsertStatement(result);
        sess.sql(insert_stmt).execute();
        auto row = sess.sql(new_query).execute().fetchOne();
    }
    //cout<< "SQL with index found count: " <<row[0].get<long>()<<endl;
    auto end = clock();
    return end - begin;
}

int main(int argc, char** argv)
{


    if (argc<3){
        print_usage();
        exit(EXIT_SUCCESS);
    }
    int long_index =0, opt= 0;
    int num_hash_buckets = 0, N=0;
    std::string index_dir, mysql_url, schema, table, column;
    while((opt=getopt_long_only(argc, argv, "", long_options, &long_index))!=-1){
        switch (opt) {
            case 'i' :
                index_dir = optarg;
                break;
            case 'm' :
                mysql_url = optarg;
                break;
            case 's' :
                schema = optarg;
                break;
            case 't' :
                table = optarg;
                break;
            case 'c' :
                column = optarg;
                break;
        }
    }

    // mysql_url: mysql://user:password@server:port/

    vector<std::string> split_result;
    split(split_result, mysql_url, is_any_of(":/@"), token_compress_on);
    assert(split_result[0]=="mysqlx");

    auto mysql_user = split_result[1], mysql_password = split_result[2], mysql_server = split_result[3], mysql_port = split_result[4];

    Session sess(mysql_server, atoi(mysql_port.c_str()), mysql_user, mysql_password);
    Schema db = sess.getSchema(schema);

    sess.sql("set global net_read_timeout = 1024000").execute();
    sess.sql("set global mysqlx_idle_worker_thread_timeout = 1024000").execute();
    sess.sql("set global mysqlx_read_timeout = 1024000").execute();
    sess.sql("set global mysqlx_write_timeout = 1024000").execute();

    sess.sql("use " + schema).execute();

    sess.sql("create temporary table index_query_result(id integer NOT NULL PRIMARY KEY)").execute();

    auto db_table = db.getTable(table, true);

    auto rocksdb_dir = index_dir + "/" + schema + "/" + table + "/" + column;

    ifstream conf;
    conf.open(rocksdb_dir + "/index.config");
    //read the configuration
    char line[256];
    while (not conf.eof()) {
        conf.getline(line, 256);
        std::string strline = line;
        if(strline.starts_with("N=")){
            N=atoi(line+2);
        }
        else if(strline.starts_with("NUM_HASH_BUCKETS=")){
            num_hash_buckets = atoi(line+17);
        }
    }
    assert(N>0);

    NcharIndex index(N, rocksdb_dir, num_hash_buckets);

    auto search_keywords =gettop1000frequentwords(index_dir);

    ofstream logf((index_dir+"/log.txt").c_str());

    for(auto str:search_keywords){

        try {

            std::string query =
                    "select count(distinct id) from " + table + " where " + column + " like binary '%" + str + "%'";
            long count=0;
            auto db_query_time = timeQueryingDatabase(sess, query,count);
            vector<ID> result;

            std::string new_query = "select count(distinct t.id) from index_query_result i straight_join " + table +
                                    " t on i.id=t.id and " + column + " like binary '%" + str + "%'";
            auto index_query_time = timeQueryingIndex(sess, new_query, index, str, result);

            // put result into temporary table
            sess.sql("truncate table index_query_result").execute();
            cout << query << endl;
            cout << "Query time with MySQL:" << db_query_time << endl;
            cout << new_query << endl;
            cout << "Query time with MySQL/ncharIndex:" << index_query_time << endl;

            logf << str << ',' <<count<<','<< db_query_time << ',' << index_query_time << endl;
        }
        catch(...)
        {
            continue;
        }
    }
    logf.close();
    return EXIT_SUCCESS;
}
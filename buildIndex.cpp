#include <iostream>
#include <fstream>
#include <cstdlib>
#include <getopt.h>
#include <bits/getopt_ext.h>

#include <mysql-cppconn-8/mysqlx/xdevapi.h>
#include <boost/algorithm/string.hpp>


#include "NcharIndex.h"
using namespace std;
using namespace mysqlx;
using namespace boost;
/*
int main() {
    system("rm -r ./db");//remove existing db from last run
    NcharIndex index(3,"./db",8);
    auto begin = time(nullptr);
    index.batchAddFromCSVFile("../mi_idtext.csv");
    auto end = time(nullptr);
    cout<<"Index build time: "<<end-begin;
// n=2, unordered_map db: 292
// n=2, map db: 400
// n=2, original: 700s, 3.9GB db size
// n=3, 10 hash buckets: 1074s, 5.2GB db size
// n=3, 8 hash buckets: 691s, 4.7GB db size
// n=3, original: 1882s, 7.2GB db size

}
 */

// Connect to a MySQL database and build indexes from there
// Usage: buildindex --output-dir index_dir --mysql-server mysql://user:password@server:port/ --schema schema_name --table table_name --column column_name --N N [--num-buckets num_buckets]
// it queries "select id, column_name from table_name"
// cursor through the query one row over another
// the rocksdb will be at index_dir/schema/table/column
// there will be a yaml file in this rocksdb directory specifying the number of buckets

void print_usage()
{
    cout<<"Usage: buildIndex --output-dir index_dir --mysql-url mysqlx://user:password@server:port/ --schema schema_name --table table_name --column column_name --N N [--num-hash-buckets num_hash_buckets]"<<endl;
}

static struct option long_options[] = {
    {"output-dir", required_argument, NULL, 'o'},
    {"mysql-url", required_argument, NULL, 'm'},
    {"schema", required_argument, NULL, 's'},
    {"table", required_argument, NULL, 't'},
    {"column", required_argument, NULL, 'c'},
    {"num-hash-buckets", optional_argument, NULL, 'b'},
    {"N", required_argument, NULL, 'n'},
    {NULL, 0, NULL, 0}
};

int main(int argc, char **argv){

    auto begin = time(nullptr);

    if (argc<3){
        print_usage();
        exit(EXIT_SUCCESS);
    }
    int long_index =0, opt= 0;
    int num_hash_buckets = 0, N=0;
    std::string output_dir, mysql_url, schema, table, column;
    while((opt=getopt_long_only(argc, argv, "", long_options, &long_index))!=-1){
        switch (opt) {
            case 'o' :
                output_dir = optarg;
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
            case 'b':
                num_hash_buckets = atoi(optarg);
                break;
            case 'n':
                N = atoi(optarg);
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

    auto db_table = db.getTable(table, true);

    //instead of using cursor (meet with a weird session expire or connection error problem)
    //we first export as CSV

    /*
    char intermediate_filename[L_tmpnam+5];
    tmpnam(intermediate_filename);
    strcat(intermediate_filename,".csv");

    cout<<intermediate_filename<<endl;

    ostringstream query;

    query<< "select id,"<<column<< " from "<<table<< " into outfile \""<<intermediate_filename << "\"";

    query << " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'";

    cout<<query.str()<<endl;

    sess.sql(query.str()).execute();

    auto time_exported = time(nullptr);

    cout<<"data export time: "<<time_exported-begin<<endl;

     */

    auto db_dir = output_dir+"/"+schema+"/"+table+"/"+column;
    system(("rm -r " + db_dir).c_str()); //clear the db from past runs
    system(("mkdir -p " + db_dir).c_str());

    NcharIndex index(N, db_dir, num_hash_buckets);

    /*
    index.batchAddFromCSVFile(intermediate_filename);

    remove(intermediate_filename);
     */
    auto execute = db_table.select("id",column).execute();
    auto rows = execute.fetchAll();

    // index_dir/schema/table/column

    size_t count_line=0;

    size_t count_error_line=0;

    for (auto row:rows){

        ID id = row[0].get<ID>();
        std::string text = row[1].get<std::string>();
        index.addToBatch(id,text);

        count_line+=1;

        if(count_line%100000==0){
            cout<<count_line/1000<< "K ";
            auto buffer_size = index.estimateBatchBufferSize();
            if (buffer_size>index.RAM_MB_LIMIT*1048576) {
                //empty every vector into DB
                cout<<"Syncing..."<<endl;
                index.commitBatch();
            }
        }

    }
    cout<<"Syncing..."<<endl;
    index.commitBatch();
    cout<<"Compacting..."<<endl;
    index.compactDB();

    auto end = time(nullptr);

    cout<<"Index build time: "<<end-begin<<" seconds\n";
    cout<<"Index size: "<<endl;
    cout.flush();
    system(("du -sh " + db_dir).c_str());

    //put N in N.txt
    ofstream fn;
    fn.open(db_dir + "/index.config");
    fn<<"N="<<N<<endl;
    fn<<"NUM_HASH_BUCKETS="<<num_hash_buckets<<endl;
    fn.close();

    return EXIT_SUCCESS;
}
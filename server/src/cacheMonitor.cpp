#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <myproto/kvstore.grpc.pb.h>
#include <rocksdb/db.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::Key;
using kvstore::Value;
using kvstore::KeyValue;
using kvstore::Empty;
using kvstore::CacheMonitor;

class CacheMonitorImpl final : public CacheMonitor::Service {
public:
    explicit CacheMonitorImpl() {
        // rocksdb::Options options;
        // options.create_if_missing = true;
        // rocksdb::DB* raw_db;
        // rocksdb::Status status = rocksdb::DB::Open(options, db_path, &raw_db);
        // if (!status.ok()) {
        //     std::cerr << "Error opening database: " << status.ToString() << std::endl;
        //     exit(1);
        // }
        // db_.reset(raw_db);
        // std::cout << "db open successful\n";
    }
    
    Status ShareCacheState(ServerContext* context, const KeyValue* CacheState, Empty* response) override {
    //     std::cout << "put received\n";
    //     rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request->key().key(), request->value().value());
    //     std::cout << "status: " << status.ToString() <<"\n";
    //     if (!status.ok()) {
    //         std::cout << "status not ok\n";
    //         return Status(grpc::StatusCode::INTERNAL, "Error putting key-value pair into database");
    //     }
    //     std::cout << "status ok\n";
        return Status::OK;
    }


private:
};

void RunCacheMonitor(string server_list_fn) {
    // std::string server_address("0.0.0.0:50051");
    // KeyValueStoreImpl service(db_path);
    // ServerBuilder builder;
    // builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // builder.RegisterService(&service);
    // std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <server_list_txt_file_name>" << std::endl;
        return 1;
    }

    RunServer(argv[1]);
    return 0;
}

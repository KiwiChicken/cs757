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

class KeyValueStoreImpl final : public KeyValueStore::Service {
public:
    explicit KeyValueStoreImpl(const std::string& db_path) {
        rocksdb::Options options;
        options.create_if_missing = true;
        auto raw_ptr = db_.get();
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &raw_ptr);
        if (!status.ok()) {
            std::cerr << "Error opening database: " << status.ToString() << std::endl;
            exit(1);
        }
    }

    Status Put(ServerContext* context, const KeyValue* request, Empty* response) override {
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request->key().key(), request->value().value());
        if (!status.ok()) {
            return Status(grpc::StatusCode::INTERNAL, "Error putting key-value pair into database");
        }
        return Status::OK;
    }

    Status Get(ServerContext* context, const Key* request, Value* response) override {
        std::string value;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), request->key(), &value);
        if (status.IsNotFound()) {
            return Status(grpc::StatusCode::NOT_FOUND, "Key not found");
        } else if (!status.ok()) {
            return Status(grpc::StatusCode::INTERNAL, "Error retrieving value from database");
        }
        response->set_value(value);
        return Status::OK;
    }

private:
    std::unique_ptr<rocksdb::DB> db_;
};

void RunServer(const std::string& db_path) {
    std::string server_address("0.0.0.0:50051");
    KeyValueStoreImpl service(db_path);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <path_to_database>" << std::endl;
        return 1;
    }

    RunServer(argv[1]);
    return 0;
}

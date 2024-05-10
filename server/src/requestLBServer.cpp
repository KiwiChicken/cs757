#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <myproto/kvstore.grpc.pb.h>
#include <rocksdb/db.h>
// #include <spdlog/spdlog.h>
// #include <spdlog/sinks/basic_file_sink.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::Key;
using kvstore::Value;
using kvstore::KeyValue;
using kvstore::Empty;

const int replicaNum = 2;

class KeyValueStoreImpl final : public KeyValueStore::Service {
public:
    explicit KeyValueStoreImpl(const std::string& db_path, const std::vector<std::string>& srvrs) {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* raw_db;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &raw_db);
        if (!status.ok()) {
            std::cerr << "Error opening database: " << status.ToString() << std::endl;
            throw std::runtime_error("Error opening database");
        }
        db_.reset(raw_db);
        std::cout << "db open successful\n";
        servers = srvrs;
        // spdlog::set_level(spdlog::level::trace);
        // spdlog::debug("This message should be displayed..");
        // logger = spdlog::basic_logger_mt("basic_logger", "logs.txt");
    }

    Status Put(ServerContext* context, const KeyValue* request, Empty* response) override {
        // spdlog::get("basic_logger")->info("Put received");
        std::lock_guard<std::mutex> lock(db_mutex_);
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request->key().key(), request->value().value());
        // spdlog::get("basic_logger")->info("Status: {}", status.ToString());
        if (!status.ok()) {
            // spdlog::get("basic_logger")->error("Status not ok");
            return Status(grpc::StatusCode::INTERNAL, "Error putting key-value pair into database");
        }
        // spdlog::get("basic_logger")->info("Status ok");
        return Status::OK;
    }

    Status Get(ServerContext* context, const Key* request, Value* response) override {
        // spdlog::get("basic_logger")->info("Get received");
        std::string value;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), request->key(), &value);
        // spdlog::get("basic_logger")->info("Status: {}", status.ToString());
        if (status.IsNotFound()) {
            // spdlog::get("basic_logger")->warn("Key not found");
            return Status(grpc::StatusCode::NOT_FOUND, "Key not found");
        } else if (!status.ok()) {
            // spdlog::get("basic_logger")->error("Status not ok");
            return Status(grpc::StatusCode::INTERNAL, "Error retrieving value from database");
        }
        response->set_value(value);
        // spdlog::get("basic_logger")->info("Status ok");
        return Status::OK;
    }

private:
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<std::string> servers;
    uint8_t offset = 0;
    std::mutex db_mutex_;
    // std::shared_ptr<spdlog::logger> logger;
};

std::vector<std::string> readServerInfo(const std::string& fn, int n) {
    std::cout << "reading server info..." << std::endl;
    std::ifstream file(fn);
    std::vector<std::string> lines(replicaNum);
    // int line_count = 0;
    std::string line;
    // while (std::getline(file, line)) {
    //     ++line_count;
    // }
    file.clear();
    file.seekg(0, std::ios::beg);
    for (int i = 0; i < n + replicaNum; ++i) {
        if (!std::getline(file, line)) {
            file.clear();
            file.seekg(0, std::ios::beg);
            std::getline(file, line);
        }
        if (i >= n) {
            // Remove newline character if it exists
            if (!line.empty() && line.back() == '\n') {
                line.pop_back();
            }
            std::cout << i << std::endl;
            std::cout << line << std::endl;
            lines[i - n] = line;
        }
    }
    return lines;
}

void RunServer(const std::string& db_path, const std::string& fn, int line_number) {
    std::vector<std::string> servers = readServerInfo(fn, line_number);
    for (auto s : servers)
        std::cout << s << std::endl;
    std::string server_address(servers[0]);
    KeyValueStoreImpl service(db_path, servers);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}


int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <path_to_database> <server_list_txt_file_name> <server_id>" << std::endl;
        return 1;
    }
    std::string filename = argv[2];
    int line_number = std::stoi(argv[3]);

    RunServer(argv[1], filename, line_number);
    return 0;
}

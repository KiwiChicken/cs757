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
using grpc::ClientContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::Key;
using kvstore::Value;
using kvstore::KeyValue;
using kvstore::Empty;

const int replicaNum = 2;

class KeyValueStoreImpl final : public KeyValueStore::Service {
public:
    explicit KeyValueStoreImpl(const std::string& db_path, const std::vector<std::string>& srvrs, int server_count, const std::string& key_min, const std::string& key_max, int id) {
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
        server_id = id;
        computePrimaryKeyRange(key_min, key_max, server_count);
        // spdlog::set_level(spdlog::level::trace);
        // spdlog::debug("This message should be displayed..");
        // logger = spdlog::basic_logger_mt("basic_logger", "logs.txt");
    }

    Status Put(ServerContext* context, const KeyValue* request, Empty* response) override {
        if (first_put) [[unlikely]] {
            connectToServers();
            first_put = false;
        }
        // spdlog::get("basic_logger")->info("Put received");
        // std::lock_guard<std::mutex> lock(db_mutex_);
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request->key().key(), request->value().value());
        // spdlog::get("basic_logger")->info("Status: {}", status.ToString());
        if (!status.ok()) {
            // spdlog::get("basic_logger")->error("Status not ok");
            return Status(grpc::StatusCode::INTERNAL, "Error putting key-value pair into database");
        }
        // spdlog::get("basic_logger")->info("Status ok");
        if (isPrimary(request->key().key())) {
            fwdPut(request->key().key(), request->value().value());
        }
        
        return Status::OK;
    }
    

    Status Get(ServerContext* context, const Key* request, Value* response) override {
        std::string value;
        // spdlog::get("basic_logger")->info("Get received");
        if (isPrimary(request->key()) && offset % replicaNum != 0) {
            value = fwdGet(request->key());
        }
        else {
            rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), request->key(), &value);
            // spdlog::get("basic_logger")->info("Status: {}", status.ToString());
            if (status.IsNotFound()) {
                // spdlog::get("basic_logger")->warn("Key not found");
                return Status(grpc::StatusCode::NOT_FOUND, "Key not found");
            } else if (!status.ok()) {
                // spdlog::get("basic_logger")->error("Status not ok");
                return Status(grpc::StatusCode::INTERNAL, "Error retrieving value from database");
            }
        }

        response->set_value(value);
        offset++;
        // spdlog::get("basic_logger")->info("Status ok");
        return Status::OK;
    }

private:
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<std::shared_ptr<KeyValueStore::Stub>> stubs_;
    uint8_t offset = 0;
    std::vector<std::string> servers;
    // std::mutex db_mutex_;
    // std::shared_ptr<spdlog::logger> logger;
    int key_start, key_end;
    int server_id;
    bool first_put = true;

    void connectToServers() {
        
        auto it = servers.begin();
        if (it != servers.end()) {
            ++it;  // Skip self
        }
        for (; it != servers.end(); ++it) {
            std::cout << "setup connection to " << *it << std::endl;
            stubs_.push_back(KeyValueStore::NewStub(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials())));
            // auto channel = grpc::CreateChannel(*it, grpc::InsecureChannelCredentials());
            // if (channel->WaitForConnected(gpr_time_add(
            //         gpr_now(GPR_CLOCK_REALTIME),
            //         gpr_time_from_seconds(10, GPR_TIMESPAN)))) {
            //     // The channel is connected. You can now call RPC methods.
            //     stubs_.push_back(KeyValueStore::NewStub(channel));
            // } else {
            //     // The channel is not connected within the given deadline.
            //     std::cerr << "Timeout connecting to peer server: " << *it << std::endl;
            //     throw std::runtime_error("Timeout connecting to peer server");
            // }
        }

        // while (true) {
        // auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        // if (channel->WaitForConnected(gpr_time_add(
        //         gpr_now(GPR_CLOCK_REALTIME),
        //         gpr_time_from_seconds(10, GPR_TIMESPAN)))) {
        //     // The channel is connected. You can now call RPC methods.
        //     stubs_.push_back(KeyValueStore::NewStub(channel));
        // } else {
        //     // The channel is not connected within the given deadline.
        //     std::cerr << "Timeout connecting to peer server: " << server_address << std::endl;
        //     throw std::runtime_error("Timeout connecting to peer server");
        // }
        // }
    }

    void computePrimaryKeyRange(const std::string& key_min, const std::string& key_max, int n) {
        int start = std::stoi(key_min);
        int end = std::stoi(key_max);
        int step = (end - start) / n;
        key_start = start + server_id * step;
        key_end = key_start + step;

        return;
    }

    bool isPrimary(const std::string& key) {
        int key_int = std::stoi(key);
        return key_int > key_start && key_int < key_end;
    }

    bool fwdPut(const std::string& key, const std::string& value) const{
        KeyValue kv;
        kv.mutable_key()->set_key(key);
        kv.mutable_value()->set_value(value);
        Empty response;
        ClientContext context;
        for (auto s : stubs_) {
            Status status = s->Put(&context, kv, &response);
            if (!status.ok()) {
                std::cerr << "Put RPC failed: " << status.error_message() << std::endl;
                return false;
            }
            
        }
        return true;
    }

    std::string fwdGet(const std::string& key) const{
        Key k;
        k.set_key(key);
        Value v;
        ClientContext context;
        int s_id = offset % (replicaNum-1);
        std::cout << s_id << std::endl;
        Status status = stubs_[s_id]->Get(&context, k, &v);
        if (status.ok()) {
            return v.value();
        } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
            return "Key not found";
        } else {
            std::cerr << "Get RPC failed: " << status.error_message() << std::endl;
            return "";
        }
    }
};

int server_count = 0;
std::vector<std::string> readServerInfo(const std::string& fn, int n) {
    std::cout << "reading server info..." << std::endl;
    std::ifstream file(fn);
    std::vector<std::string> lines(replicaNum);
    // int line_count = 0;
    std::string line;
    while (std::getline(file, line)) {
        ++server_count;
    }
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

void RunServer(const std::string& db_path, const std::string& fn, int server_id, const std::string& key_min, const std::string& key_max) {
    std::vector<std::string> servers = readServerInfo(fn, server_id);
    for (auto s : servers)
        std::cout << s << std::endl;
    std::string server_address(servers[0]);
    KeyValueStoreImpl service(db_path, servers, server_count, key_min, key_max, server_id);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}


int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <path_to_database> <server_list_txt_file_name> <server_id> <key_min> <key_max>" << std::endl;
        return 1;
    }
    std::string filename = argv[2];
    int server_id = std::stoi(argv[3]);

    RunServer(argv[1], filename, server_id, argv[4], argv[5]);
    return 0;
}

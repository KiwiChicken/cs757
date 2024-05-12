#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <myproto/kvstore.grpc.pb.h>
#include <rocksdb/db.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <thread>
#include <chrono>
// #include <glog/logging.h>

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
using kvstore::ServerLoad;
using kvstore::request;

const int replicaNum = 2;


class KeyValueStoreImpl final : public KeyValueStore::Service {
public:
    // uint32_t req_cnt_epoch = 0;
    std::vector<uint32_t> replica_load; // TODO: seems these should be atomic, but will have compile error
    long epoch_start_ms = 0;
    explicit KeyValueStoreImpl(const std::string& db_path, const std::vector<std::string>& srvrs, int srvr_cnt, 
    const std::string& key_min, const std::string& key_max, int id) : replica_load(replicaNum, 0) {
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
        server_count = srvr_cnt;
        computePrimaryKeyRange(key_min, key_max, server_count);
        thread_ = std::thread([this] { this->SendEpochInfo(); });
    }

    ~KeyValueStoreImpl() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    Status Put(ServerContext* context, const KeyValue* request, Empty* response) override {
        if (first_put) [[unlikely]] {
            connectToServers();
            first_put = false;
        }
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request->key().key(), request->value().value());
        if (!status.ok()) {
            return Status(grpc::StatusCode::INTERNAL, "Error putting key-value pair into database");
        }
        if (isPrimary(request->key().key())) {
            fwdPut(request->key().key(), request->value().value());
        }
        return Status::OK;
    }

    Status Get(ServerContext* context, const Key* request, Value* response) override {
        std::string value;
        int server_choice = isPrimary(request->key()) ? pickServer() : 0;
        std::cout << "Get with key: " << request->key() <<" server choice: " << server_choice << std::endl;
        if (server_choice != 0) {
            value = fwdGet(request->key(), server_choice-1);
        }
        else {
            auto timestamp_it = context->client_metadata().find("timestamp");
            std::string timestamp_str(timestamp_it->second.begin(), timestamp_it->second.end());
            long timestamp = std::stol(timestamp_str);
            std::cout << "local get " << request->key() << "with timestamp " << timestamp << std::endl;
            if (epoch_start_ms - timestamp >= 100) [[unlikely]] {
                epoch_start_ms = timestamp;
                replica_load[0] = 0;
            }
            replica_load[0]++;

            rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), request->key(), &value);
            if (status.IsNotFound()) {
                std::cerr << "Key not found with key: " << request->key() << std::endl;
                return Status(grpc::StatusCode::NOT_FOUND, "Key not found");
            } else if (!status.ok()) {
                std::cerr << "Error retrieving value from database with key: " << request->key() << std::endl;
                return Status(grpc::StatusCode::INTERNAL, "Error retrieving value from database");
            }
            std::cout << "local get successful with key: " << request->key() << std::endl;
        }
        response->set_value(value);
        replica_load[server_choice]++;
        return Status::OK;
    }

    Status UpdateEpochInfo(ServerContext* context, const ServerLoad* request, Empty* response) {
        replica_load[(request->server_id() - server_id) % server_count] = request->req_cnt();
        return Status::OK;
    }
private:
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<std::shared_ptr<KeyValueStore::Stub>> stubs_;
    std::vector<std::string> servers;
    int key_start, key_end;
    uint32_t server_id;
    int server_count;
    bool first_put = true;
    std::thread thread_;

    void connectToServers() {
        
        auto it = servers.begin();
        if (it != servers.end()) {
            ++it;  // Skip self
        }
        for (; it != servers.end(); ++it) {
            std::cout << "setup connection to " << *it << std::endl;
            stubs_.push_back(KeyValueStore::NewStub(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials())));
        }
    }

    void computePrimaryKeyRange(const std::string& key_min, const std::string& key_max, int n) {
        int start = std::stoi(key_min);
        int end = std::stoi(key_max);
        int step = (end - start) / n;
        key_start = start + server_id * step;
        key_end = key_start + step;
        std::cout << "computePrimaryKeyRange start=" << key_start << " end=" << key_end << std::endl;
        return;
    }

    bool isPrimary(const std::string& key) {
        int key_int = std::stoi(key);
        std::cout << "isPrimary? " << (key_int >= key_start && key_int < key_end) << std::endl;
        return key_int >= key_start && key_int < key_end;
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

    std::string fwdGet(const std::string& key, int server_choice) const{
        Key k;
        k.set_key(key);
        Value v;
        ClientContext context;
        // Get the current time
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        context.AddMetadata("timestamp", std::to_string(milliseconds));
        std::cout << "forward get to server " << server_choice+server_id << std::endl;
        Status status = stubs_[server_choice]->Get(&context, k, &v);
        if (status.ok()) {
            return v.value();
        } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
            return "Key not found";
        } else {
            std::cerr << "Get RPC failed: " << status.error_message() << std::endl;
            return "";
        }
    }

    int pickServer() {
        int min = INT_MAX;
        int min_id = 0;
        for (int i = 0; i < replicaNum; ++i) {
            if (replica_load[i] < min) {
                min = replica_load[i];
                min_id = i;
            }
        }
        std::cout << "pickServer found min load " << min << std::endl;
        return min_id;
    }

    void SendEpochInfo() {
        while (true) {
            for (const auto& stub : stubs_) {
                ServerLoad request;
                request.set_server_id(server_id);
                request.set_req_cnt(replica_load[0]);
                Empty response;
                grpc::ClientContext context;

                // Set up your request here...

                grpc::Status status = stub->UpdateEpochInfo(&context, request, &response);

                if (status.ok()) {
                    // Handle the response here...
                } else {
                    std::cout << "RPC failed" << std::endl;
                }
            }

            // Wait for 1 second before sending the next round of requests
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
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
    // Add the interceptor factory
    std::unique_ptr<Server> server(builder.BuildAndStart());
    // std::cout << "Server listening on " << server_address << std::endl;
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}


int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <path_to_database> <server_list_txt_file_name> <server_id> <key_min> <key_max>" << std::endl;
        return 1;
    }
    // google::InitGoogleLogging(argv[0]);
    std::string filename = argv[2];
    int server_id = std::stoi(argv[3]);

    RunServer(argv[1], filename, server_id, argv[4], argv[5]);
    return 0;
}

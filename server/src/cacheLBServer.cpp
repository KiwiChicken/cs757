#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <list>
#include <unordered_set>
#include <grpcpp/grpcpp.h>
#include <myproto/kvstore.grpc.pb.h>
#include <rocksdb/db.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <thread>
#include <chrono>
#include <rocksdb/statistics.h>

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
using kvstore::CacheState;

const u_int8_t replicaNum = 2;
const u_int64_t CACHE_SIZE_KB = 8 * 1024;
const u_int64_t KV_SIZE_KB = 100;
const u_int8_t MISS_PERCENT = 20;
const u_int8_t INSERT_PERCENT = 20;

class LRUCache {
public:
    LRUCache(size_t max_size) : max_size(max_size) {}

    void insert(const std::string& key) {
        // If the key is already in the cache, update its usage and return.
        auto it = cache.find(key);
        if (it != cache.end()) {
            usage.remove(*it);
            usage.push_front(key);
            return;
        }

        // If the cache is full, remove the least recently used key.
        if (cache.size() == max_size) {
            cache.erase(usage.back());
            usage.pop_back();
        }

        // Insert the key into the cache and update its usage.
        usage.push_front(key);
        cache.insert( *(usage.begin()) );
    }

    bool contains(const std::string& key) {
        return cache.find(key) != cache.end();
    }

    const std::list<std::string>& getKeysInOrderOfUsage() const {
        return usage;
    }

private:
    size_t max_size;
    std::list<std::string> usage;
    std::unordered_set<std::string> cache;
};

class KeyValueStoreImpl final : public KeyValueStore::Service {
public:
    explicit KeyValueStoreImpl(const std::string& db_path, const std::vector<std::string>& rep_srvrs, const std::vector<std::string>& prm_srvrs,
    int srvr_cnt, const std::string& key_min, const std::string& key_max, int id) 
    : replica_load(replicaNum, 0), replica_miss_percentile(replicaNum, 0), replica_insert_percentile(replicaNum, 0), hot_keys(CACHE_SIZE_KB/KV_SIZE_KB/10) {
        options.create_if_missing = true;
        options.statistics = rocksdb::CreateDBStatistics();
        rocksdb::DB* raw_db;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &raw_db);
        if (!status.ok()) {
            std::cerr << "Error opening database: " << status.ToString() << std::endl;
            throw std::runtime_error("Error opening database");
        }
        db_.reset(raw_db);
        std::cout << "db open successful\n";
        replica_servers = rep_srvrs;
        primary_servers = prm_srvrs;
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
        int server_choice = isPrimary(request->key()) ? pickServer(request->key()) : 0;
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
        hot_keys.insert(request->key());
        return Status::OK;
    }

    Status ShareCacheState(ServerContext* context, const CacheState* request, Empty* response) {
        int idx = (request->server_id() - server_id) % server_count;
        replica_load[idx] = request->req_cnt();
        uint64_t total_reqs = request->cache_data_hit() +  request->cache_data_miss();
        replica_miss_percentile[idx] =  static_cast<uint8_t>(static_cast<double>(request->cache_data_miss()) * 100 / total_reqs);
        replica_insert_percentile[idx] = static_cast<uint8_t>(static_cast<double>(request->cache_data_bytes_insert()) * 100 / total_reqs);
        std::unordered_set<std::string> new_hot_keys(request->hot_keys().begin(),  request->hot_keys().end());
        replica_hot_keys[idx] = new_hot_keys;
        return Status::OK;
    }
private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options;
    std::vector<std::shared_ptr<KeyValueStore::Stub>> stubs_;
    std::vector<std::shared_ptr<KeyValueStore::Stub>> cacheLBstubs_;
    std::vector<std::string> replica_servers;
    std::vector<std::string> primary_servers;
    int key_start, key_end;
    uint32_t server_id;
    int server_count;
    bool first_put = true;
    std::vector<uint32_t> replica_load; // TODO: seems these should be atomic, but will have compile error
    std::vector<uint8_t> replica_miss_percentile;
    std::vector<uint8_t> replica_insert_percentile;
    std::vector<std::unordered_set<std::string>> replica_hot_keys;
    LRUCache hot_keys;    // we set cache to 8 MB and each kv is ~100 KB, so ~top 10%
    long epoch_start_ms = 0;
    std::thread thread_;

    void connectToServers() {
        
        auto it = replica_servers.begin();
        if (it != replica_servers.end()) {
            ++it;  // Skip self
        }
        for (; it != replica_servers.end(); ++it) {
            std::cout << "setup connection to " << *it << std::endl;
            stubs_.push_back(KeyValueStore::NewStub(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials())));
        }

        it = primary_servers.begin();
        for (; it != primary_servers.end(); ++it) {
            std::cout << "setup connection to " << *it << std::endl;
            cacheLBstubs_.push_back(KeyValueStore::NewStub(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials())));
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

    bool isValidServer(int idx, const std::string& key) {
        bool is_hot = replica_hot_keys[idx].find(key) != replica_hot_keys[idx].end();
        std::cout << replica_miss_percentile[idx] << std::endl;
        bool low_miss = replica_miss_percentile[idx] < MISS_PERCENT;
        std::cout << replica_insert_percentile[idx] << std::endl;
        bool low_write = replica_insert_percentile[idx] < INSERT_PERCENT;
        return is_hot && low_miss && low_write;
    }
    // same as requestLB, but pick out those with high miss/insert and key is not in hot cache
    int pickServer(const std::string& key) {
        int min = INT_MAX;
        int min_id = 0;
        for (int i = 0; i < replicaNum; ++i) {
            std::cout << i << std::endl;
            if (replica_load[i] < min) {
                if (i > 1 && !isValidServer(i-1, key)) continue;
                min = replica_load[i];
                min_id = i;
            }
        }
        std::cout << "pickServer found min load " << min << std::endl;
        return min_id;
    }

    void SendEpochInfo() {
        while (true) {
            for (const auto& stub : cacheLBstubs_) {
                CacheState request;
                uint64_t cache_data_hit = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
                uint64_t cache_data_miss = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
                uint64_t cache_data_bytes_insert = options.statistics->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_BYTES_INSERT);
                request.set_server_id(server_id);
                request.set_req_cnt(replica_load[0]);
                request.set_cache_data_hit(cache_data_hit);
                request.set_cache_data_miss(cache_data_miss);
                request.set_cache_data_bytes_insert(cache_data_bytes_insert);
                std::list<std::string> keys = hot_keys.getKeysInOrderOfUsage();
                for (const auto& key : keys) {
                    request.add_hot_keys(key);
                }
                Empty response;
                grpc::ClientContext context;

                // Set up your request here...

                grpc::Status status = stub->ShareCacheState(&context, request, &response);

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
std::vector<std::string> readReplicaServerInfo(const std::string& fn, int n) {
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

std::vector<std::string> readPrimaryServerInfo(const std::string& fn, int n) {
    std::cout << "reading primary server info..." << std::endl;
    int first_prime_id = (n - replicaNum) % server_count;
    std::ifstream file(fn);
    std::vector<std::string> lines(replicaNum - 1);
    std::string line;
    int i = 0, j = 0;
    for (int i = 0; i < server_count; ++i) {
        if (!std::getline(file, line)) {
            file.clear();
            file.seekg(0, std::ios::beg);
            std::getline(file, line);
        }
        if ( (i > first_prime_id && (i < n || first_prime_id > n) ) || i > n - replicaNum) {
            if (!line.empty() && line.back() == '\n') {
                line.pop_back();
            }
            std::cout << i << std::endl;
            std::cout << line << std::endl;
            lines[j] = line;
            j++;
        }
    }
    return lines;
}

void RunServer(const std::string& db_path, const std::string& fn, int server_id, const std::string& key_min, const std::string& key_max) {
    std::vector<std::string> replica_servers = readReplicaServerInfo(fn, server_id);
    std::vector<std::string> primary_servers = readPrimaryServerInfo(fn, server_id);
    std::string server_address(replica_servers[0]);
    KeyValueStoreImpl service(db_path, replica_servers, primary_servers, server_count, key_min, key_max, server_id);
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

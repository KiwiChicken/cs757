// kvstore_client.cpp

#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <myproto/kvstore.grpc.pb.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::Key;
using kvstore::Value;
using kvstore::KeyValue;
using kvstore::Empty;


class KeyValueStoreClient {
public:
    KeyValueStoreClient(const std::vector<std::string>& servers, int server_count, const std::string& key_min, const std::string& key_max) {
        for (const auto& server : servers) {
            stubs_.push_back(KeyValueStore::NewStub(grpc::CreateChannel(server, grpc::InsecureChannelCredentials())));
        }
        computeKeyRanges(key_min, key_max, server_count);
    }

    bool Put(const std::string& key, const std::string& value) const{
        KeyValue kv;
        kv.mutable_key()->set_key(key);
        kv.mutable_value()->set_value(value);
        Empty response;
        ClientContext context;
        int s_id = computeServerIndex(key);
        std::cout << s_id << std::endl;
        Status status = stubs_[s_id]->Put(&context, kv, &response);
        if (status.ok()) {
            return true;
        } else {
            std::cerr << "Put RPC failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    std::string Get(const std::string& key) const{
        Key k;
        k.set_key(key);
        Value v;
        ClientContext context;
        int s_id = computeServerIndex(key);
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

private:
    std::vector<std::unique_ptr<KeyValueStore::Stub>> stubs_;
    std::vector<int> keyRanges;

    void computeKeyRanges(const std::string& key_min, const std::string& key_max, int n) {
        int start = std::stoi(key_min);
        int end = std::stoi(key_max);
        int step = (end - start) / n;
        for (int i = 1; i < n; ++i) {
            keyRanges.push_back(start + i * step);
        }

        return;
    }

    int computeServerIndex(const std::string& key) const{
        int k = std::stoi(key);
        for (int i = 0; i < keyRanges.size(); ++i) {
            if (k < keyRanges[i]) {
                return i;
            }
        }
        return keyRanges.size() - 1;
    }
};



int server_count = 0;
std::vector<std::string> readServerInfo(const std::string& fn) {
    std::cout << "reading server info..." << std::endl;
    std::ifstream file(fn);
    std::string line;
    while (std::getline(file, line)) {
        ++server_count;
    }
    std::cout << server_count << std::endl;
    std::vector<std::string> lines(server_count);
    file.clear();
    file.seekg(0, std::ios::beg);
    for (int i = 0; i < server_count; ++i) {
        std::cout << i << std::endl;
        if (std::getline(file, line)) {
            if (!line.empty() && line.back() == '\n') {
                line.pop_back();
            }
            std::cout << line << std::endl;
            lines[i] = line;
        }
    }
    return lines;
}
void playTrace(const KeyValueStoreClient& client, const std::string& fn) {
    std::ifstream file(fn);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string key, value, request_type;
            if (std::getline(ss, key, ',') && std::getline(ss, value, ',') && std::getline(ss, request_type)) {
                // Trim trailing newline if it exists
                if (!request_type.empty() && request_type.back() == '\n') {
                    request_type.pop_back();
                }
                std::cout << line << std::endl;
                if (request_type == "PUT") {
                    client.Put(key, value);
                } else if (request_type == "GET") {
                    client.Get(key);
                } else {
                    std::cerr << "Invalid request type: " << request_type << std::endl;
                }
                
            }
        }
    }
}

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <server_list_txt_file_name> <trace_csv_file_name> <key_min> <key_max>" << std::endl;
        return 1;
    }

    std::string serverfn = argv[1];
    std::string tracefn = argv[2];
    std::string key_min = argv[3];
    std::string key_max = argv[4];

    std::vector<std::string> servers = readServerInfo(serverfn);
    KeyValueStoreClient client(servers, server_count, key_min, key_max);

    playTrace(client, tracefn);
    // std::string key(argv[2]);
    // std::string value(argv[3]);

    // KeyValueStoreClient client(grpc::CreateChannel(
    //     server_address, grpc::InsecureChannelCredentials()));

    // client.Put(key, value);
    // std::cout << "Put key-value pair: " << key << ": "<< value << std::endl;

    // value = client.Get(key);
    // std::cout << "Get value for key " << key << ": " << value << std::endl;

    return 0;
}

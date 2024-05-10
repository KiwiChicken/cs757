// kvstore_client.cpp

#include <iostream>
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
    KeyValueStoreClient(std::shared_ptr<Channel> channel)
        : stub_(KeyValueStore::NewStub(channel)) {}

    bool Put(const std::string& key, const std::string& value) {
        KeyValue kv;
        kv.mutable_key()->set_key(key);
        kv.mutable_value()->set_value(value);
        Empty response;
        ClientContext context;
        Status status = stub_->Put(&context, kv, &response);
        if (status.ok()) {
            return true;
        } else {
            std::cerr << "Put RPC failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    std::string Get(const std::string& key) {
        Key k;
        k.set_key(key);
        Value v;
        ClientContext context;
        Status status = stub_->Get(&context, k, &v);
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
    std::unique_ptr<KeyValueStore::Stub> stub_;
};


void computeKeyRange(const std::string& db_path) {
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <server_list_txt_file_name> <key> <value>" << std::endl;
        return 1;
    }

    std::string server_address(argv[1]);
    std::string key(argv[2]);
    std::string value(argv[3]);

    KeyValueStoreClient client(grpc::CreateChannel(
        server_address, grpc::InsecureChannelCredentials()));

    client.Put(key, value);
    std::cout << "Put key-value pair: " << key << ": "<< value << std::endl;

    value = client.Get(key);
    std::cout << "Get value for key " << key << ": " << value << std::endl;

    return 0;
}

#include "sio_client.h"
#include "sio_socket.h"

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>

int main(int argc, char** argv) {
    sio::client client;
    std::promise<bool> result;
    std::atomic<bool> completed{false};

    auto finish = [&](bool success) {
        if (!completed.exchange(true)) {
            result.set_value(success);
        }
    };

    client.set_fail_listener([&]() {
        finish(false);
    });
    client.set_socket_open_listener([&](const std::string& namespaceName) {
        std::cout << "namespace opened: " << namespaceName << '\n';
        if (namespaceName != "/custom") {
            return;
        }

        auto bytes = std::make_shared<const std::string>("\0\1\xff", 3);
        sio::message::list arguments;
        arguments.push(sio::string_message::create("cpp"));
        arguments.push(sio::binary_message::create(bytes));
        client.socket("/custom")->emit(
            "message-with-ack",
            arguments,
            [&](const sio::message::list& acknowledgement) {
                finish(
                    acknowledgement.size() == 2 &&
                    acknowledgement[0]->get_string() == "cpp" &&
                    *acknowledgement[1]->get_binary() == std::string("\0\1\xff", 3));
            });
    });
    client.set_open_listener([&]() {
        client.socket("/custom");
    });

    client.connect(argc > 1 ? argv[1] : "http://127.0.0.1:3000");
    auto future = result.get_future();
    bool success = future.wait_for(std::chrono::seconds(5)) == std::future_status::ready &&
                   future.get();
    client.sync_close();

    std::cout << (success ? "C++ namespace + binary acknowledgement: PASS" : "C++ integration: FAIL") << '\n';
    return success ? 0 : 1;
}

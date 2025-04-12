#include "src/include/MPMCRingBuffer.hpp"
#include <thread>
#include <chrono>
#include <iostream>

// Custom consumer implementation
struct EventProcessor : public MPMCRingBuffer<int>::Consumer {
    bool consume(const int& event) override {
        std::cout << event << std::endl;
        return true;
    }
};

int main() {
    MPMCRingBuffer<int> buffer;
    auto consumer = std::make_shared<EventProcessor>();
    buffer.register_consumer(consumer);

    Producer<int> producer(buffer);

    // Producer thread
    std::thread producer_thread([&producer] {
        for (int i = 0; i < 1000; ++i) {
            producer.enqueue(i);
        }
    });

    // Dispatcher thread
    std::thread dispatcher([&buffer] {
        while (true) {
            buffer.dispatch_events();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    producer_thread.join();
    dispatcher.detach();

    return 0;
}
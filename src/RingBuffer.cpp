#include "src/include/SPSCRingBuffer.hpp"
#include <chrono>
#include <iostream>
#include <thread>

class SimpleConsumer : public SPSCRingBuffer<int>::Consumer {
public:
  void consume(const int &event) override {
    std::cout << "Consumed: " << event << std::endl;
  }
};

int main() {
  SPSCRingBuffer<int> buffer(1024);
  SimpleConsumer consumer;

  // Producer thread
  std::thread producer([&buffer]() {
    for (int i = 0; i < 100; ++i) {
      while (!buffer.try_enqueue(i)) {
        std::this_thread::yield();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  // Consumer thread
  std::thread consumer_thread([&buffer]() {
    int value;
    while (true) {
      if (buffer.try_dequeue(value)) {
        std::cout << "Consumed: " << value << std::endl;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  });

  producer.join();
  consumer_thread.detach();

  return 0;
}
#ifndef MPMCRINGBUFFER_HPP
#define MPMCRINGBUFFER_HPP

#include <atomic>
#include <memory>
#include <vector>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/thread/synchronized_value.hpp>
#include <boost/align/aligned_allocator.hpp>

template<typename T>
class MPMCRingBuffer {
public:
    // Consumer interface
    class Consumer {
    public:
        virtual ~Consumer() = default;
        Consumer(const Consumer&) = delete;
        Consumer& operator=(const Consumer&) = delete;
        Consumer(Consumer&&) = delete;
        Consumer& operator=(Consumer&&) = delete;
        virtual bool consume(const T& event) = 0;
    };

    explicit MPMCRingBuffer(size_t initial_size = 1024) {
        resize(initial_size);
    }

    void resize(size_t new_size) {
        Node* new_nodes = allocator_.allocate(new_size);
        
        // Initialize nodes as linked list
        for (size_t i = 0; i < new_size - 1; ++i) {
            new_nodes[i].next.store(&new_nodes[i+1], std::memory_order_relaxed);
        }
        new_nodes[new_size-1].next.store(nullptr, std::memory_order_relaxed);
    
        head_.store(new_nodes, std::memory_order_release);
        tail_.store(new_nodes, std::memory_order_release);
        capacity_ = new_size;
    }

    ~MPMCRingBuffer() {
        Node* current = head_.load(std::memory_order_relaxed);
        if (current) {
            allocator_.deallocate(current, capacity_);
        }
    }

    // Disable copy and move
    MPMCRingBuffer(const MPMCRingBuffer&) = delete;
    MPMCRingBuffer& operator=(const MPMCRingBuffer&) = delete;
    MPMCRingBuffer(MPMCRingBuffer&&) = delete;
    MPMCRingBuffer& operator=(MPMCRingBuffer&&) = delete;

    // Consumer registration
    void register_consumer(std::shared_ptr<Consumer> consumer);

    // Public interface for event dispatch
    void dispatch_events();

    // Producer API
    bool enqueue(const T& event);

private:
    // Internal node structure
    struct Node {
        T data;
        std::atomic<Node*> next{nullptr};
    };

    // Aligned allocator for cache line optimization
    using Allocator = boost::alignment::aligned_allocator<Node, 64>;
    Allocator allocator_;

    // Producer-side members with cache line alignment
    alignas(64) std::atomic<Node*> head_{nullptr};
    alignas(64) std::atomic<Node*> tail_{nullptr};
    std::atomic<size_t> size_{0};
    size_t capacity_{0};

    // Thread-safe consumer registry
    boost::synchronized_value<std::vector<std::shared_ptr<Consumer>>> consumers_;

    // Buffer management
    void resize(size_t new_size);
    void expand_buffer();
    bool try_enqueue(const T& event);
};

// Producer class
template<typename T>
class Producer {
public:
    explicit Producer(MPMCRingBuffer<T>& buffer) : buffer_(buffer) {}

    bool enqueue(const T& event) {
        return buffer_.enqueue(event);
    }

private:
    MPMCRingBuffer<T>& buffer_;
};

#include "MPMCRingBuffer.ipp"

#endif // MPMCRINGBUFFER_HPP
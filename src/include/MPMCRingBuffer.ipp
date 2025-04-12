#ifndef MPMCRINGBUFFER_IPP
#define MPMCRINGBUFFER_IPP

// Implementation of MPMCRingBuffer member functions

#include "MPMCRingBuffer.hpp"
template<typename T>
MPMCRingBuffer<T>::MPMCRingBuffer(size_t initial_size) {
    resize(initial_size);
}

template<typename T>
MPMCRingBuffer<T>::~MPMCRingBuffer() {
    Node* current = head_.load(std::memory_order_relaxed);
    if (current) {
        allocator_.deallocate(current, capacity_);
    }
}

template<typename T>
void MPMCRingBuffer<T>::resize(size_t new_size) {
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

template<typename T>
bool MPMCRingBuffer<T>::try_enqueue(const T& event) {
    Node* tail = tail_.load(std::memory_order_acquire);
    Node* next = tail->next.load(std::memory_order_relaxed);

    if (next) {
        tail_.store(next, std::memory_order_release);
        tail->data = event;
        return true;
    }
    return false;
}

template<typename T>
void MPMCRingBuffer<T>::expand_buffer() {
    size_t new_capacity = capacity_ * 2;
    Node* new_head = allocator_.allocate(new_capacity);

    // Initialize new nodes
    for (size_t i = 0; i < new_capacity - 1; ++i) {
        new_head[i].next.store(&new_head[i+1], std::memory_order_relaxed);
    }
    new_head[new_capacity-1].next.store(nullptr, std::memory_order_relaxed);

    Node* old_head = head_.load(std::memory_order_acquire);
    Node* expected = old_head;

    if (head_.compare_exchange_strong(expected, new_head,
                                    std::memory_order_acq_rel)) {
        // Link old tail to new head
        Node* old_tail = tail_.load(std::memory_order_acquire);
        old_tail->next.store(new_head, std::memory_order_release);
        allocator_.deallocate(old_head, capacity_);
        capacity_ = new_capacity;
    } else {
        allocator_.deallocate(new_head, new_capacity);
    }
}

template<typename T>
bool MPMCRingBuffer<T>::enqueue(const T& event) {
    if (try_enqueue(event)) return true;
    expand_buffer();
    return try_enqueue(event);
}

template<typename T>
void MPMCRingBuffer<T>::register_consumer(std::shared_ptr<Consumer> consumer) {
    typename boost::synchronized_value<std::vector<std::shared_ptr<Consumer>>>::scoped_lock lock(consumers_);
    lock->emplace_back(std::move(consumer));
}

template<typename T>
void MPMCRingBuffer<T>::dispatch_events() {
    Node* current_head = head_.load(std::memory_order_acquire);
    Node* current = current_head;

    while (Node* next = current->next.load(std::memory_order_acquire)) {
        auto consumers = consumers_.synchronize();
        for (auto& consumer : *consumers) {
            consumer->consume(next->data);
        }
        current = next;
    }

    head_.store(current, std::memory_order_release);
}

#endif // MPMCRINGBUFFER_IPP
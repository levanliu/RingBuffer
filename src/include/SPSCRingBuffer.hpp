#ifndef SPSCRINGBUFFER_HPP
#define SPSCRINGBUFFER_HPP

#include <atomic>
#include <boost/align/aligned_allocator.hpp>
#include <cstddef>
#include <memory>

template <typename T> class SPSCRingBuffer {
public:
  // Consumer interface
  class Consumer {
  public:
    virtual ~Consumer() = default;
    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;
    Consumer(Consumer &&) = delete;
    Consumer &operator=(Consumer &&) = delete;

    virtual void consume(const T &event) = 0;

  protected:
    Consumer() = default;
  };

  explicit SPSCRingBuffer(size_t capacity = 1024)
      : capacity_(capacity), buffer_(allocator_.allocate(capacity)),
        mask_(capacity - 1) {
    // Ensure capacity is a power of 2
    if ((capacity & (capacity - 1)) != 0) {
      throw std::invalid_argument("Capacity must be a power of 2");
    }
  }

  ~SPSCRingBuffer() {
    if (buffer_) {
      allocator_.deallocate(buffer_, capacity_);
    }
  }

  // Disable copy and move
  SPSCRingBuffer(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer &operator=(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer(SPSCRingBuffer &&) = delete;
  SPSCRingBuffer &operator=(SPSCRingBuffer &&) = delete;

  // Producer API
  bool try_enqueue(const T &event) {
    const size_t current_write = write_index_.load(std::memory_order_relaxed);
    const size_t next_write = (current_write + 1) & mask_;

    if (next_write == read_index_.load(std::memory_order_acquire)) {
      return false; // Buffer is full
    }

    buffer_[current_write] = event;
    write_index_.store(next_write, std::memory_order_release);
    return true;
  }

  // Consumer API
  bool try_dequeue(T &event) {
    const size_t current_read = read_index_.load(std::memory_order_relaxed);

    if (current_read == write_index_.load(std::memory_order_acquire)) {
      return false; // Buffer is empty
    }

    event = buffer_[current_read];
    read_index_.store((current_read + 1) & mask_, std::memory_order_release);
    return true;
  }

  bool empty() const {
    return read_index_.load(std::memory_order_acquire) ==
           write_index_.load(std::memory_order_acquire);
  }

  bool full() const {
    const size_t next_write =
        (write_index_.load(std::memory_order_acquire) + 1) & mask_;
    return next_write == read_index_.load(std::memory_order_acquire);
  }

  size_t size() const {
    size_t write = write_index_.load(std::memory_order_acquire);
    size_t read = read_index_.load(std::memory_order_acquire);
    return (write - read) & mask_;
  }

private:
  // Aligned allocator for cache line optimization
  using Allocator = boost::alignment::aligned_allocator<T, 64>;
  Allocator allocator_;

  const size_t capacity_;
  T *const buffer_;
  const size_t mask_;

  // Cache line aligned indices
  alignas(64) std::atomic<size_t> write_index_{0};
  alignas(64) std::atomic<size_t> read_index_{0};
};

#endif // SPSCRINGBUFFER_HPP
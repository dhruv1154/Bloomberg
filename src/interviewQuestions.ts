import { TriviaQuestion } from "./types";

export const INTERVIEW_QUESTIONS: TriviaQuestion[] = [
  {
    id: "hft-1",
    category: "OS_HARDWARE",
    title: "CPU Core Pinning & Cache Thrashing Prevention",
    difficulty: "HARD",
    question: "What is thread affinity (core pinning) in low-latency C++/Rust systems? How does the OS scheduler affect P99 latency, and what happens to physical caches (L1/L2) during a thread context switch?",
    explanation: "Under standard Linux scheduling, threads are multiplexed across arbitrary CPU cores. When a thread is migrated from Core A to Core B: \n1. Its hot registers, Program Counter (PC), and stack pointer are saved, and the OS loads Core B's instruction task index. This execution state swap is called a context switch.\n2. More destructively, the instruction cache (I-cache) and data cache (D-cache) on the active L1 and L2 lines of Core B are cold. Core B must fetch references from L3 or Main RAM, driving latency from ~1ns to 100ns+. This is 'cache thrashing'.\n\nBy leveraging affinity calls (like pthread_setaffinity_np in C++ or core_affinity in Rust), we tell the Linux Scheduler to completely isolate our execution tasks. No other user-space processes compete on that pinned core, protecting the pre-allocated CPU caches and ensuring consistent sub-microsecond matching.",
    answerCode: `// C++ Low Latency Core Pinning Example
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <iostream>

void pin_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    
    pthread_t current_thread = pthread_self();
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "CORE AFFINITY ASSIGNMENT FAILED FOR CORE #" << core_id << "\\n";
    } else {
        std::cout << "CORE PINNED SUCCESS TO PHYSICAL CORE #" << core_id << "\\n";
    }
}`,
    interviewTips: "Firms like Citadel and Optiver love asking how you isolate cores. Mention 'CPU isolation via isolcpus kernel parameter', 'avoiding standard OS scheduling interrupts', and 'pinned lock-free SPSC buffer architecture'."
  },
  {
    id: "hft-2",
    category: "OS_HARDWARE",
    title: "Avoiding Cache-Line False Sharing",
    difficulty: "EXPERT",
    question: "What is 'False Sharing' in modern multi-threaded architectures, and how can hardware cache-line alignment prevent atomic contention spikes on the L1/L2 cache bus?",
    explanation: "In HFT systems, different threads frequently write to independent variables. However, memory is loaded into CPU caches in blocks of 64 bytes called 'cache lines'. \n\nIf Thread 1 modifies variable 'A' and Thread 2 modifies variable 'B', and both sits on the same 64-byte segment: \n1. Whenever Thread 1 writes to A, the hardware's Intel MESI (Modified, Exclusive, Shared, Invalid) coherency database marks the entire L1 cache line on Thread 2's core as INVALID.\n2. Thread 2's core must halt and refresh the entire cache line from L3 cache or RAM, even though Thread 2 never touched A!\n\nTo prevent this false sharing, HFT structures use cache-alignment flags. We insert padding bytes or align our structs to 64-byte boundaries, ensuring each core operates on entirely distinct cache-line segments.",
    answerCode: `// Rust Cache Line Padding Example
#[repr(align(64))] // Align struct to 64-byte boundary
pub struct CacheAlignedBufferSlot<T> {
    pub value: T,
    // Compiler inserts zero-overhead padding bytes 
    // to fill up the remaining cache lines
}

// C++11 Equivalent
struct alignas(64) AlignedSlot {
    uint64_t target_order_price;
    uint32_t fill_quantity;
    char pad[48]; // Padding to ensure alignment
};`,
    interviewTips: "Interviewers will ask how to identify false sharing (with performance counters like 'perf c2c' or 'cache-misses' measurements). Highlight aligning core structures to hardware page/cache line boundaries."
  },
  {
    id: "lang-1",
    category: "MARKET_STRUCTURE",
    title: "Price-Time Priority Limit Order Book Data-Structures",
    difficulty: "HARD",
    question: "What raw data structures are optimal for implementing a Price-Time Priority Limit Order Book (LOB) in high-frequency matching engines? How do you achieve O(1) order inserts, pointer cancels, and trade execution?",
    explanation: "To match trades in real-time, the Order Book must maintain orders sorted by Price (first priority), and then by Arrival Time (second priority).\n\nAn optimal structure consists of:\n1. A doubly-linked list of dynamic Order nodes at each price level (limit). This maintains time priority. Appending a new order or deleting a cancelled order is O(1).\n2. A Red-Black tree or B-Tree of limit levels, mapped by price. Bids are sorted descending, Asks ascending. Finding the best bid/ask is O(1), and traversing levels is O(log N).\n3. A Hash Map (e.g. hash table or sparse array indexed by OrderID). This stores pointers directly to the Order nodes inside the doubly linked lists. This unlocks O(1) instantaneous Order Cancel (Lookup -> Remove Node).",
    answerCode: `// Optimal LOB Core Architecture Setup
struct Order {
    uint64_t order_id;
    uint32_t price;
    uint32_t qty;
    Order* prev;
    Order* next;
};

struct LimitLevel {
    uint32_t price;
    uint32_t total_volume;
    Order* head_order;
    Order* tail_order;
};

// Top matching container using hash map + price tree
// std::map<uint32_t, LimitLevel*> bid_levels; // price sorted
// std::unordered_map<uint64_t, Order*> order_map; // instantaneous O(1) cancellation`,
    interviewTips: "Firms like Jane Street or Jump Trading expect you to explain why a single vector or binary search list is bad (insertions/cancels become O(N) shift operations, which is lethal in volatile market swings)."
  },
  {
    id: "lang-2",
    category: "LANG_OPTIMIZATION",
    title: "Dynamic Allocation & Heap Bypassing in Rust/C++",
    difficulty: "MEDIUM",
    question: "Why is standard memory allocation (malloc / new) banned inside low-latency execution paths? How do standard Object Pools and flat-array pre-allocations circumvent memory fragmentation?",
    explanation: "When you call 'malloc' or 'new', the application requests memory block management from the OS Allocator. The allocator must look up thread-local caches, search the virtual memory heap using locks, and occasionally trigger a system call (sbrk or mmap) to request pages from the Linux kernel. This has a highly unpredictable execution time, ranging from 100 nanoseconds to 2 milliseconds.\n\nTo ensure deterministic latency, low-latency applications preallocate all variables, structures, and arrays inside custom 'Object Pools' on application startup in the heap. During live streams, variable lookup and recycling drops to basic static index increments, completely avoiding heap context overheads.",
    answerCode: `// Zero-Allocation Memory Arena/Pool in C++
template <typename T, size_t PoolSize>
class PreallocatedPool {
private:
    T pool_storage[PoolSize];
    size_t next_free_idx = 0;

public:
    T* acquire() {
        if (next_free_idx >= PoolSize) {
            throw std::runtime_error("POOL SATURATION!");
        }
        return &pool_storage[next_free_idx++];
    }

    void reset() {
        next_free_idx = 0; // fast O(1) recycle of memory slots
    }
};`,
    interviewTips: "Contrast dynamic heap memory with CPU Stack registers and static preallocation block pools. Emphasize that HFT = flat memory layouts + cache locality."
  },
  {
    id: "net-1",
    category: "NETWORKING",
    title: "Kernel Bypass & TCP Socket Acceleration (Solarflare EF_VI)",
    difficulty: "HARD",
    question: "How does Kernel Bypassing (e.g. Solarflare EF_VI, DPDK, or OpenOnload) reduce networking latency in electronic market matching streams? Contrast it with the standard Linux network stack.",
    explanation: "In a standard Linux network transaction:\n1. The network interface card (NIC) receives a physical Ethernet packet.\n2. The NIC triggers a hardware interrupt. The CPU halts its active process, swaps into Kernel Mode, and routes the packet through the TCP/IP stack (dev_queue, sk_buff buffers).\n3. Finally, the OS wake up the user-space process via select/poll/epoll, and the client calls dynamic 'sys_recv'. The kernel copies packet data into our application buffer. This round-trip takes 5 - 20 microseconds.\n\nKernel Bypass mechanisms maps the memory regions of the PCIe NIC directly into our user-space memory arena. The application directly polls the PCIe ring buffer registers. No hardware interrupts, no context-switches, and zero buffer copies. Latency drops to sub-microsecond levels.",
    answerCode: `// Low Latency Kernel Bypass Socket Polling Pseudocode
void run_kernel_bypass_loop() {
    ef_driver_handle driver;
    ef_ring_buffer rx_ring;
    
    // Direct PCIe ring register polling
    while (true) {
        int packets_rcvd = ef_poll_rx_buffer(&rx_ring);
        if (packets_rcvd > 0) {
            // Direct zero-copy access to ethernet packet memory
            char* payload = ef_get_packet_payload(&rx_ring);
            process_market_data(payload);
            
            // Instantly recycle slot to network card
            ef_recycle_buffer(&rx_ring);
        }
    }
}`,
    interviewTips: "Firms like Optiver use Solarflare NIC cards globally. Highlighting DPDK or EF_VI polling shows you know physical packet ingestion rather than just reading basic books."
  },
  {
    id: "net-2",
    category: "NETWORKING",
    title: "TCP vs UDP in Exchanges (TCP Out-of-Order Recovery)",
    difficulty: "HARD",
    question: "Market data feeds (e.g. Nasdaq ITCH, Binance Websockets) typically run on UDP Multicast or accelerated TCP. How does an HFT client handle TCP 'Head-of-Line Blocking' (HOL) and recover lost sequence frames without halting the book?",
    explanation: "TCP is a stream-oriented protocol that guarantees packet order delivery. If packet #101 is lost in the network network wires, TCP will halt delivery of packets #102 and #103 until packet #101 is re-requested and retransmitted by the sender. This is 'Head-of-Line Blocking' and introduces devastating latency spikes.\n\nTo bypass this, professional exchanges stream market state feeds over UDP Multicast (using two distinct network feeds: Feed A and Feed B). If a sequence packet goes missing on Feed A, the HFT client checks if Feed B received it. If both missed it, the client launches a specialized fast sideband TCP/IP retransmission request socket to retrieve the gap, while continuing to cache newly arrived frames in a thread-safe lockbox.",
    answerCode: `// Feed A / Feed B UDP Sequence Alignment Loop
struct MarketUpdate {
    uint64_t sequence_id;
    char data[128];
};

void sync_multicast_feeds(MarketUpdate pkt_a, MarketUpdate pkt_b, uint64_t& expected_seq) {
    if (pkt_a.sequence_id == expected_seq) {
        process_update(pkt_a);
        expected_seq++;
    } else if (pkt_b.sequence_id == expected_seq) {
        process_update(pkt_b);
        expected_seq++;
    } else {
        // Gap Detected! Register gap and request accelerated retransmission
        trigger_tcp_gap_retransmit(expected_seq, pkt_a.sequence_id - 1);
        expected_seq = pkt_a.sequence_id + 1; // forward state
    }
}`,
    interviewTips: "Mention that UDP multicast supports Feed A and Feed B path synchronization, allowing clients to run racing readers on separate hardware lanes to bypass local network switch dropouts."
  }
];

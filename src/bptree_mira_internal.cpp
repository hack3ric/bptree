#include "../../Mira/runtime/libcommon2/include/cache.hpp"
#include <cstdint>
#include <vector>

#ifndef BPTREE_MIRA_LOCAL
#include "../../Mira/runtime/libcommon2/include/rvector.h"
#include <iostream>
#endif

// Cache configuration example taken from Mira Dataframe

// 85 bytes * 512 * 1024 one block
const size_t s1_nb = 524288;
// 24 -> 1G
const size_t s1_n_block = 24;

// token offset, raddr offset, laddr offset, slots, slot size bytes, id
using rbpn = DirectCache<0, 0, 0, s1_n_block, s1_nb * sizeof(uint64_t), 0>;
using rbpn_R = CacheReq<rbpn>;

// std::vector<bptree_node_t> *node_pool;
std::vector<uint64_t> *node_pool2;

extern "C" {
int cache_request_impl_1(int qid, uint64_t tag, int offset, bool send) {
  return rbpn_R::cache_request_impl(qid, tag, offset, NULL, send);
}

void poll_qid1(int offset, uint16_t seq) {
  poll_qid(offset, seq);
}
}

void ext_init() {
#ifndef BPTREE_MIRA_LOCAL
  init_client();
#endif

//   node_pool = new std::vector<bptree_node_t>();
//   node_pool->reserve(1048576);
//   node_pool->push_back({}); // insert one empty value as NULL
// #ifndef BPTREE_MIRA_LOCAL
//   new_remotelize<bptree_node_t, rbpn, rbpn_R>(*node_pool, true);
//   std::cout << "Mira: B+ tree node pool manually remotelized" << std::endl;
// #endif

  node_pool2 = new std::vector<uint64_t>();
  node_pool2->reserve(1048576 * 128);
  node_pool2->resize(1024 / sizeof(uint64_t),
                     0); // insert one empty value as NULL
#ifndef BPTREE_MIRA_LOCAL
  new_remotelize<uint64_t, rbpn, rbpn_R>(*node_pool2, true);
  std::cout << "Mira: B+ tree node pool manually remotelized" << std::endl;
#endif
}

void reserve_node_locks(std::vector<pthread_rwlock_t>& locks) {
  locks.reserve(1048576);
  locks.resize(1);
}

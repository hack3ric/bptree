#include "../../Mira/runtime/libcommon2/include/cache.hpp"
// #include "../../Mira/runtime/libcommon2/include/rvector.h"
#include "bptree.h"
#include <vector>

// Cache configuration example taken from Mira Dataframe

// 85 bytes * 512 * 1024 one block
const size_t s1_nb = 524288;
// 24 -> 1G
const size_t s1_n_block = 24;

// token offset, raddr offset, laddr offset, slots, slot size bytes, id
using rbpn = DirectCache<0, 0, 0, s1_n_block, s1_nb * sizeof(bptree_node_t), 0>;
using rbpn_R = CacheReq<rbpn>;

std::vector<bptree_node_t> *node_pool;

void ext_init() {
  node_pool = new std::vector<bptree_node_t>();
  node_pool->reserve(1048576);
  node_pool->push_back({});  // insert one empty value as NULL
#ifndef BPTREE_MIRA_LOCAL
  #include <iostream>
  new_remotelize<bptree_node_t, rbpn, rbpn_R>(*node_pool, true);
  std::cout << "Mira: B+ tree node pool manually remotelized" << std::endl;
#endif
}

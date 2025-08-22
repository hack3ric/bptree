#ifndef BPTREE_MIRA_INTERNAL_H
#define BPTREE_MIRA_INTERNAL_H

#include <cstdint>
#include <vector>

// [[deprecated("Use node_pool2 instead")]]
// extern std::vector<bptree_node_t> *node_pool;
extern std::vector<uint64_t> *node_pool2;

void ext_init();

#endif // BPTREE_MIRA_INTERNAL_H

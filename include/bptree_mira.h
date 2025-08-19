#ifndef BPTREE_H
#define BPTREE_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#define ORDER 60 // Order of B+ tree (maximum number of keys in a node)

#ifdef __cplusplus
extern "C" {
#endif

typedef struct bptree_node {
  bool is_leaf;
  int num_keys;
  uint64_t keys[ORDER - 1];
  union {
    uint64_t values[ORDER - 1];          // For leaf nodes
    size_t children[ORDER]; // For internal nodes
  };
  size_t next; // For leaf nodes to form a linked list
  pthread_rwlock_t lock;    // Reader-writer lock for concurrency control
} bptree_node_t;

typedef struct {
  size_t root;
  pthread_rwlock_t root_lock;
} bptree_t;

// Initialize a new B+ tree
void bptree_init(void);

// Insert a key-value pair into the tree
bool bptree_insert(uint64_t key, uint64_t value);

// Search for a key in the tree, returns true if found and sets value
bool bptree_search(uint64_t key, uint64_t *value);

// Delete a key from the tree
bool bptree_delete(uint64_t key);

// Clean up the tree and free all memory
void bptree_destroy(void);

#ifdef __cplusplus
}
#else
#ifndef static_assert
#define static_assert _Static_assert
#endif
#endif // __cplusplus

static_assert(sizeof(bptree_node_t) <= 1024);

#endif // BPTREE_H

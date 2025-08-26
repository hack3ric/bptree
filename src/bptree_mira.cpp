#include "bptree_mira.h"
#include "../../Mira/runtime/libcommon2/include/rvector.h"
#include "bptree_mira_internal.hpp"
#include <cstdint>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#define U64_PER_NODE (1024 / sizeof(uint64_t))

static std::vector<pthread_rwlock_t> node_locks;

void reserve_node_locks(std::vector<pthread_rwlock_t>& locks);

extern "C" {
static inline size_t pool_size() {
  auto &pool = *node_pool2;
  return pool.size() / 128; // using U64_PER_NODE fails cgeist
}

static inline bptree_node_t *get_node(bptree_node_ref_t node) {
  auto &pool = *node_pool2;
  if (!node)
    return nullptr;
  return (bptree_node_t *)&pool[node * U64_PER_NODE];
}

static inline bool node_is_leaf(bptree_node_ref_t node) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE];
}

static inline void node_set_is_leaf(bptree_node_ref_t node, bool is_leaf) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE] = is_leaf; // >> 56
}

static inline int node_num_keys(bptree_node_ref_t node) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE + 1];
}

static inline void node_set_num_keys(bptree_node_ref_t node, int num_keys) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE + 1] = num_keys;
}

static inline uint64_t node_key(bptree_node_ref_t node, size_t idx) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE + 2 + idx];
}

static inline void node_set_key(bptree_node_ref_t node, size_t idx, uint64_t key) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE + 2 + idx] = key;
}

static inline uint64_t node_value(bptree_node_ref_t node, size_t idx) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE + 2 + (ORDER - 1) + idx];
}

static inline void node_set_value(bptree_node_ref_t node, size_t idx, uint64_t value) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE + 2 + (ORDER - 1) + idx] = value;
}

static inline bptree_node_ref_t node_child(bptree_node_ref_t node, size_t idx) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE + 2 + (ORDER - 1) + idx];
}

static inline void node_set_child(bptree_node_ref_t node, size_t idx, bptree_node_ref_t child) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE + 2 + (ORDER - 1) + idx] = child;
}

static inline uint64_t node_next(bptree_node_ref_t node) {
  auto &pool = *node_pool2;
  return pool[node * U64_PER_NODE + 2 + (ORDER - 1) + ORDER];
}

static inline void node_set_next(bptree_node_ref_t node, bptree_node_ref_t next) {
  auto &pool = *node_pool2;
  pool[node * U64_PER_NODE + 2 + (ORDER - 1) + ORDER] = next;
}

static inline pthread_rwlock_t* node_lock(bptree_node_ref_t node) {
  return &node_locks[node];
}

static void pool_set_size(size_t s) {
  auto &v = *node_pool2;
  rvector<uint64_t> *rv = (rvector<uint64_t> *)&v;
  size_t c = v.capacity();
  if (s > c) {
    printf("Size larger than cap, dont do this\n");
    exit(1);
  }
  rv->end = rv->head + s;
}

static void locks_set_size(size_t s) {
  rvector<pthread_rwlock_t> *rv = (rvector<pthread_rwlock_t> *)&node_locks;
  size_t c = node_locks.capacity();
  if (s > c) {
    printf("Size larger than cap, dont do this\n");
    exit(1);
  }
  rv->end = rv->head + s;
}

static inline bptree_node_t *alloc_node() {
  auto &pool = *node_pool2;
  size_t size = pool_size();
  pool_set_size((size + 1) *
                U64_PER_NODE); // pool.resize((size + 1) * U64_PER_NODE);
  locks_set_size(size + 1);
  pthread_rwlock_init(&node_locks[size], NULL);
  return (bptree_node_t *)&pool[size * U64_PER_NODE];
}

static pthread_mutex_t alloc_mutex;

static bptree_node_ref_t bptree_internal_create_node(bool is_leaf) {
  pthread_mutex_lock(&alloc_mutex);
  bptree_node_t *node = alloc_node();
  // memset(node, 0, sizeof(bptree_node_t));
  // node->is_leaf = is_leaf;
  // node->num_keys = 0;
  // pthread_rwlock_init(&node->lock, NULL); // TODO: move rwlock to a new static vector
  bptree_node_ref_t result = pool_size() - 1;
  node_set_is_leaf(result, is_leaf);
  node_set_num_keys(result, 0);
  pthread_mutex_unlock(&alloc_mutex);
  return result;
}

static void bptree_internal_destroy_node(bptree_node_ref_t node) {
  if (!node)
    return;
  // pthread_rwlock_destroy(&node_lock(node));
  // free(node);
}

static bptree_t bptree = {};

void bptree_init() {
  pthread_mutex_init(&alloc_mutex, NULL);
  reserve_node_locks(node_locks);

  bptree.root = bptree_internal_create_node(true); // Start with a leaf node
  pthread_rwlock_init(&bptree.root_lock, NULL);
}

static void unlock_ancestors(bptree_node_ref_t *nodes, int count) {
  for (int i = count - 1; i >= 0; i--) {
    pthread_rwlock_unlock(node_lock(nodes[i]));
  }
}

static bool search_node(bptree_node_ref_t node, uint64_t key, int *pos) {
  int left = 0, right = node_num_keys(node) - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (node_key(node, mid) == key) {
      *pos = mid;
      return true;
    }
    if (node_key(node, mid) < key)
      left = mid + 1;
    else
      right = mid - 1;
  }

  *pos = left;
  return false;
}

bool bptree_search(uint64_t key, uint64_t *value) {
  pthread_rwlock_rdlock(&bptree.root_lock);
  bptree_node_ref_t current = bptree.root;
  pthread_rwlock_rdlock(node_lock(current));
  pthread_rwlock_unlock(&bptree.root_lock);

  while (!node_is_leaf(current)) {
    int pos;
    search_node(current, key, &pos);
    if (pos == node_num_keys(current))
      pos--;

    bptree_node_ref_t next = node_value(current, pos);
    pthread_rwlock_rdlock(node_lock(next));
    pthread_rwlock_unlock(node_lock(current));
    current = next;
  }

  int pos;
  bool found = search_node(current, key, &pos);
  if (found && value) {
    *value = node_value(current, pos);
  }
  pthread_rwlock_unlock(node_lock(current));
  return found;
}

static void split_child(bptree_node_ref_t parent, int index,
                        bptree_node_ref_t child) {
  bptree_node_ref_t new_node = bptree_internal_create_node(node_is_leaf(child));
  int mid = (ORDER - 1) / 2;

  node_set_num_keys(new_node, ORDER - 1 - mid - 1);
  // memcpy(get_node(new_node)->keys, &node_key(child, mid + 1),
  //        node_num_keys(new_node) * sizeof(uint64_t));
  for (int i = 0; i < node_num_keys(new_node); i++) {
    node_set_key(new_node, i, node_key(child, mid + 1 + i));
  }

  if (node_is_leaf(child)) {
    // memcpy(get_node(new_node)->values, &node_value(child, mid + 1),
    //        node_num_keys(new_node) * sizeof(uint64_t));
    for (int i = 0; i < node_num_keys(new_node); i++) {
      node_set_value(new_node, i, node_value(child, mid + 1 + i));
    }
    node_set_next(new_node, node_next(child));
    node_set_next(child, new_node);
    node_set_num_keys(child, mid + 1);
  } else {
    // memcpy(get_node(new_node)->children, &node_value(child, mid + 1),
    //        (node_num_keys(new_node) + 1) * sizeof(bptree_node_t *));
    for (int i = 0; i <= node_num_keys(new_node); i++) {
      node_set_child(new_node, i, node_value(child, mid + 1 + i));
    }
    node_set_num_keys(child, mid);
  }

  for (int i = node_num_keys(parent); i > index; i--) {
    node_set_key(parent, i, node_key(parent, i - 1));
    node_set_child(parent, i + 1, node_value(parent, i));
  }

  node_set_key(parent, index, node_key(child, mid));
  node_set_child(parent, index + 1, new_node);
  // get_node(parent)->num_keys++;
  node_set_num_keys(parent, node_num_keys(parent) + 1);
}

bool bptree_insert(uint64_t key, uint64_t value) {
  pthread_rwlock_wrlock(&bptree.root_lock);
  bptree_node_ref_t root = bptree.root;

  if (node_num_keys(root) == ORDER - 1) {
    bptree_node_ref_t new_root = bptree_internal_create_node(false);
    bptree.root = new_root;
    node_set_child(new_root, 0, root);
    pthread_rwlock_wrlock(node_lock(new_root));
    pthread_rwlock_wrlock(node_lock(root));
    split_child(new_root, 0, root);
    root = new_root;
  } else {
    pthread_rwlock_wrlock(node_lock(root));
  }
  pthread_rwlock_unlock(&bptree.root_lock);

  bptree_node_ref_t current = root;
  bptree_node_ref_t parent = 0;
  bptree_node_ref_t ancestors[32]; // Stack for lock coupling
  int ancestor_count = 0;

  while (!node_is_leaf(current)) {
    int pos;
    search_node(current, key, &pos);
    if (pos == node_num_keys(current))
      pos--;

    ancestors[ancestor_count++] = current;
    bptree_node_ref_t child = node_value(current, pos);
    pthread_rwlock_wrlock(node_lock(child));

    if (node_num_keys(child) == ORDER - 1) {
      split_child(current, pos, child);
      if (key > node_key(current, pos)) {
        pthread_rwlock_unlock(node_lock(child));
        child = node_value(current, pos + 1);
        pthread_rwlock_wrlock(node_lock(child));
      }
    }
    current = child;
  }

  int pos;
  if (search_node(current, key, &pos)) {
    // Key already exists, update value
    node_set_value(current, pos, value);
    unlock_ancestors(ancestors, ancestor_count);
    pthread_rwlock_unlock(node_lock(current));
    return false;
  }

  // Insert into leaf node
  for (int i = node_num_keys(current) - 1; i >= pos; i--) {
    node_set_key(current, i + 1, node_key(current, i));
    node_set_value(current, i + 1, node_value(current, i));
  }

  node_set_key(current, pos, key);
  node_set_value(current, pos, value);
  node_set_num_keys(current, node_num_keys(current) + 1);

  unlock_ancestors(ancestors, ancestor_count);
  pthread_rwlock_unlock(node_lock(current));
  return true;
}

void bptree_destroy(void) {}

/*
void bptree_destroy(bptree_t *tree) {
  if (!tree || !tree->root)
    return;

  // Lock the root to prevent new operations
  pthread_rwlock_wrlock(&tree->root_lock);

  // Use dynamic queue that can grow
  size_t queue_capacity = ORDER; // Start with ORDER size
  size_t queue_size = 0;
  bptree_node_t **queue = malloc(queue_capacity * sizeof(bptree_node_t *));
  if (!queue) {
    pthread_rwlock_unlock(&tree->root_lock);
    return;
  }

  // Add root to queue
  queue[queue_size++] = tree->root;

  size_t processed = 0;
  while (processed < queue_size) {
    bptree_node_t *node = queue[processed++];

    if (!node->is_leaf) {
      // Lock the node while we access its children
      pthread_rwlock_wrlock(&node->lock);

      // Ensure queue has enough space
      if (queue_size + node->num_keys + 1 > queue_capacity) {
        size_t new_capacity = queue_capacity * 2;
        bptree_node_t **new_queue =
            realloc(queue, new_capacity * sizeof(bptree_node_t *));
        if (!new_queue) {
          pthread_rwlock_unlock(&node->lock);
          // Clean up already processed nodes
          for (size_t i = 0; i < processed; i++) {
            if (queue[i])
              bptree_internal_destroy_node(queue[i]);
          }
          free(queue);
          pthread_rwlock_unlock(&tree->root_lock);
          return;
        }
        queue = new_queue;
        queue_capacity = new_capacity;
      }

      // Add all children to queue
      for (int i = 0; i <= node->num_keys; i++) {
        if (node->children[i]) {
          queue[queue_size++] = node->children[i];
        }
      }
      pthread_rwlock_unlock(&node->lock);
    }
  }

  // Now safe to destroy all nodes
  for (size_t i = 0; i < queue_size; i++) {
    if (queue[i]) {
      bptree_internal_destroy_node(queue[i]);
    }
  }

  free(queue);
  pthread_rwlock_unlock(&tree->root_lock);
  pthread_rwlock_destroy(&tree->root_lock);
  tree->root = NULL;
}
*/

void *pth_bm_target_create() {
  ext_init();
  bptree_init();
  return (void*)1;
}

void pth_bm_target_destroy(void *target) {
  bptree_destroy();
}

void pth_bm_target_read(void *target, int key) {
  bptree_search(key, nullptr);
}

void pth_bm_target_insert(void *target, int key) {
  bptree_insert(key, 0xdeadbeef);
}

void pth_bm_target_update(void *target, int key) {
  bptree_insert(key, 0xcafecafe);
}

void pth_bm_target_delete(void *target, int key) {
}
}

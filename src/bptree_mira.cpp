/**
 * bptree.c but applied Mira hack
 */

#include "../include/bptree_mira.h"
#include "../../Mira/runtime/libcommon2/include/rvector.h"
#include "../include/bptree_mira_internal.hpp"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <vector>

extern "C" {
// TODO: make all methods accept index instead of ptr

size_t bptree_internal_create_node(bool is_leaf) {
  auto &pool = *node_pool;
  bptree_node_t node = {.is_leaf = is_leaf, .num_keys = 0};
  pthread_rwlock_init(&node.lock, NULL);

  // Mira's way of simulating std::vector::push_back
  size_t s = pool.size() + 1;
  rvector<bptree_node_t> *rpool = (rvector<bptree_node_t> *)&pool;
  size_t c = pool.capacity();
  if (s > c) {
    printf("Size larger than cap, dont do this\n");
    exit(1);
  }
  rpool->end = rpool->head + s;

  return s - 1;
}

static inline bool is_all_zero(void *val, size_t size) {
  char *val_ = (char *)val;
  return size == 0 || (val_[0] == 0 && memcmp(val_, val_ + 1, size - 1) == 0);
}

static inline void bptree_internal_destroy_node(size_t idx) {
  auto &pool = *node_pool;
  if (idx >= pool.size() || is_all_zero(&pool[idx], sizeof(bptree_node_t)))
    return;
  pthread_rwlock_destroy(&pool[idx].lock);
  memset(&pool[idx], 0, sizeof(bptree_node_t));
}

static bptree_t bptree = {};

void bptree_init(void) {
  size_t root = bptree_internal_create_node(true);
  bptree.root = root; // Start with a leaf node
  pthread_rwlock_init(&bptree.root_lock, NULL);
}

void unlock_ancestors(size_t *nodes, int count) {
  auto &pool = *node_pool;
  std::vector<int> &pool2 = *node_pool2;
  for (int i = count - 1; i >= 0; i--) {
    pthread_rwlock_unlock(&pool[nodes[i]].lock);
  }
}

static inline bool search_node(size_t idx, uint64_t key, int *pos) {
  auto &pool = *node_pool;
  int left = 0, right = pool[idx].num_keys - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (pool[idx].keys[mid] == key) {
      *pos = mid;
      return true;
    }
    if (pool[idx].keys[mid] < key)
      left = mid + 1;
    else
      right = mid - 1;
  }

  *pos = left;
  return false;
}

bool bptree_search(uint64_t key, uint64_t *value) {
  auto &pool = *node_pool;
  pthread_rwlock_rdlock(&bptree.root_lock);
  size_t current_idx = bptree.root;
  pthread_rwlock_rdlock(&pool[current_idx].lock);

  pthread_rwlock_unlock(&bptree.root_lock);

  while (!pool[current_idx].is_leaf) {
    int pos;
    search_node(current_idx, key, &pos);
    if (pos == pool[current_idx].num_keys)
      pos--;

    size_t next_idx = pool[current_idx].children[pos];
    pthread_rwlock_rdlock(&pool[next_idx].lock);
    pthread_rwlock_unlock(&pool[current_idx].lock);
    current_idx = next_idx;
  }

  int pos;
  bool found = search_node(current_idx, key, &pos);
  if (found && value) {
    *value = pool[current_idx].values[pos];
  }
  pthread_rwlock_unlock(&pool[current_idx].lock);
  return found;
}

static inline void split_child(size_t parent_idx, int index, size_t child_idx) {
  auto &pool = *node_pool;
  size_t new_node_idx = bptree_internal_create_node(pool[child_idx].is_leaf);
  int mid = (ORDER - 1) / 2;

  pool[new_node_idx].num_keys = ORDER - 1 - mid - 1;
  memcpy(pool[new_node_idx].keys, &pool[child_idx].keys[mid + 1],
         pool[new_node_idx].num_keys * sizeof(uint64_t));

  if (pool[child_idx].is_leaf) {
    memcpy(pool[new_node_idx].values, &pool[child_idx].values[mid + 1],
           pool[new_node_idx].num_keys * sizeof(uint64_t));
    pool[new_node_idx].next = pool[child_idx].next;
    pool[child_idx].next = new_node_idx;
    pool[child_idx].num_keys = mid + 1;
  } else {
    memcpy(pool[new_node_idx].children, &pool[child_idx].children[mid + 1],
           (pool[new_node_idx].num_keys + 1) * sizeof(size_t));
    pool[child_idx].num_keys = mid;
  }

  for (int i = pool[parent_idx].num_keys; i > index; i--) {
    pool[parent_idx].keys[i] = pool[parent_idx].keys[i - 1];
    pool[parent_idx].children[i + 1] = pool[parent_idx].children[i];
  }

  pool[parent_idx].keys[index] = pool[child_idx].keys[mid];
  pool[parent_idx].children[index + 1] = new_node_idx;
  pool[parent_idx].num_keys++;
}

bool bptree_insert(uint64_t key, uint64_t value) {
  auto &pool = *node_pool;
  pthread_rwlock_wrlock(&bptree.root_lock);
  size_t root_idx = bptree.root;

  if (pool[root_idx].num_keys == ORDER - 1) {
    size_t new_root_idx = bptree_internal_create_node(false);
    bptree.root = new_root_idx;
    pool[new_root_idx].children[0] = root_idx;
    pthread_rwlock_wrlock(&pool[new_root_idx].lock);
    pthread_rwlock_wrlock(&pool[root_idx].lock);
    split_child(new_root_idx, 0, root_idx);
    root_idx = new_root_idx;
  } else {
    pthread_rwlock_wrlock(&pool[root_idx].lock);
  }
  pthread_rwlock_unlock(&bptree.root_lock);

  size_t current_idx = root_idx;
  size_t ancestors[32]; // Stack for lock coupling
  int ancestor_count = 0;

  while (!pool[current_idx].is_leaf) {
    int pos;
    search_node(current_idx, key, &pos);
    if (pos == pool[current_idx].num_keys)
      pos--;

    ancestors[ancestor_count++] = current_idx;
    size_t child_idx = pool[current_idx].children[pos];
    pthread_rwlock_wrlock(&pool[child_idx].lock);

    if (pool[child_idx].num_keys == ORDER - 1) {
      split_child(current_idx, pos, child_idx);
      if (key > pool[current_idx].keys[pos]) {
        pthread_rwlock_unlock(&pool[child_idx].lock);
        child_idx = pool[current_idx].children[pos + 1];
        pthread_rwlock_wrlock(&pool[child_idx].lock);
      }
    }
    current_idx = child_idx;
  }

  int pos;
  if (search_node(current_idx, key, &pos)) {
    // Key already exists, update value
    pool[current_idx].values[pos] = value;
    unlock_ancestors(ancestors, ancestor_count);
    pthread_rwlock_unlock(&pool[current_idx].lock);
    return false;
  }

  // Insert into leaf node
  for (int i = pool[current_idx].num_keys - 1; i >= pos; i--) {
    pool[current_idx].keys[i + 1] = pool[current_idx].keys[i];
    pool[current_idx].values[i + 1] = pool[current_idx].values[i];
  }

  pool[current_idx].keys[pos] = key;
  pool[current_idx].values[pos] = value;
  pool[current_idx].num_keys++;

  unlock_ancestors(ancestors, ancestor_count);
  pthread_rwlock_unlock(&pool[current_idx].lock);
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
}

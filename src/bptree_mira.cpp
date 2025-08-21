#include "bptree_mira.h"
#include "bptree_mira_internal.hpp"
#include <stdlib.h>
#include <string.h>

extern "C" {
static bptree_node_ref_t bptree_internal_create_node(bool is_leaf) {
  bptree_node_t node = {};
  memset(&node, 0, sizeof(bptree_node_t));
  node.is_leaf = is_leaf;
  node.num_keys = 0;
  pthread_rwlock_init(&node.lock, NULL);

  auto &pool = *node_pool;
  pool.push_back(node);
  return pool.size() - 1;
}

static void bptree_internal_destroy_node(bptree_node_ref_t node) {
  if (!node)
    return;
  auto &pool = *node_pool;
  pthread_rwlock_destroy(&pool[node].lock);
  // free(node);
}

static bptree_t bptree = {};

void bptree_init() {
  bptree.root = bptree_internal_create_node(true); // Start with a leaf node
  pthread_rwlock_init(&bptree.root_lock, NULL);
}

static void unlock_ancestors(bptree_node_ref_t *nodes, int count) {
  auto &pool = *node_pool;
  for (int i = count - 1; i >= 0; i--) {
    pthread_rwlock_unlock(&pool[nodes[i]].lock);
  }
}

static bool search_node(bptree_node_ref_t node, uint64_t key, int *pos) {
  auto &pool = *node_pool;
  int left = 0, right = pool[node].num_keys - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (pool[node].keys[mid] == key) {
      *pos = mid;
      return true;
    }
    if (pool[node].keys[mid] < key)
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
  bptree_node_ref_t current = bptree.root;
  pthread_rwlock_rdlock(&pool[current].lock);
  pthread_rwlock_unlock(&bptree.root_lock);

  while (!pool[current].is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == pool[current].num_keys)
      pos--;

    bptree_node_ref_t next = pool[current].children[pos];
    pthread_rwlock_rdlock(&pool[next].lock);
    pthread_rwlock_unlock(&pool[current].lock);
    current = next;
  }

  int pos;
  bool found = search_node(current, key, &pos);
  if (found && value) {
    *value = pool[current].values[pos];
  }
  pthread_rwlock_unlock(&pool[current].lock);
  return found;
}

static void split_child(bptree_node_ref_t parent, int index,
                        bptree_node_ref_t child) {
  auto &pool = *node_pool;
  bptree_node_ref_t new_node = bptree_internal_create_node(pool[child].is_leaf);
  int mid = (ORDER - 1) / 2;

  pool[new_node].num_keys = ORDER - 1 - mid - 1;
  memcpy(pool[new_node].keys, &pool[child].keys[mid + 1],
         pool[new_node].num_keys * sizeof(uint64_t));

  if (pool[child].is_leaf) {
    memcpy(pool[new_node].values, &pool[child].values[mid + 1],
           pool[new_node].num_keys * sizeof(uint64_t));
    pool[new_node].next = pool[child].next;
    pool[child].next = new_node;
    pool[child].num_keys = mid + 1;
  } else {
    memcpy(pool[new_node].children, &pool[child].children[mid + 1],
           (pool[new_node].num_keys + 1) * sizeof(bptree_node_t *));
    pool[child].num_keys = mid;
  }

  for (int i = pool[parent].num_keys; i > index; i--) {
    pool[parent].keys[i] = pool[parent].keys[i - 1];
    pool[parent].children[i + 1] = pool[parent].children[i];
  }

  pool[parent].keys[index] = pool[child].keys[mid];
  pool[parent].children[index + 1] = new_node;
  pool[parent].num_keys++;
}

bool bptree_insert(uint64_t key, uint64_t value) {
  auto &pool = *node_pool;
  pthread_rwlock_wrlock(&bptree.root_lock);
  bptree_node_ref_t root = bptree.root;

  if (pool[root].num_keys == ORDER - 1) {
    bptree_node_ref_t new_root = bptree_internal_create_node(false);
    bptree.root = new_root;
    pool[new_root].children[0] = root;
    pthread_rwlock_wrlock(&pool[new_root].lock);
    pthread_rwlock_wrlock(&pool[root].lock);
    split_child(new_root, 0, root);
    root = new_root;
  } else {
    pthread_rwlock_wrlock(&pool[root].lock);
  }
  pthread_rwlock_unlock(&bptree.root_lock);

  bptree_node_ref_t current = root;
  bptree_node_ref_t parent = 0;
  bptree_node_ref_t ancestors[32]; // Stack for lock coupling
  int ancestor_count = 0;

  while (!pool[current].is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == pool[current].num_keys)
      pos--;

    ancestors[ancestor_count++] = current;
    bptree_node_ref_t child = pool[current].children[pos];
    pthread_rwlock_wrlock(&pool[child].lock);

    if (pool[child].num_keys == ORDER - 1) {
      split_child(current, pos, child);
      if (key > pool[current].keys[pos]) {
        pthread_rwlock_unlock(&pool[child].lock);
        child = pool[current].children[pos + 1];
        pthread_rwlock_wrlock(&pool[child].lock);
      }
    }
    current = child;
  }

  int pos;
  if (search_node(current, key, &pos)) {
    // Key already exists, update value
    pool[current].values[pos] = value;
    unlock_ancestors(ancestors, ancestor_count);
    pthread_rwlock_unlock(&pool[current].lock);
    return false;
  }

  // Insert into leaf node
  for (int i = pool[current].num_keys - 1; i >= pos; i--) {
    pool[current].keys[i + 1] = pool[current].keys[i];
    pool[current].values[i + 1] = pool[current].values[i];
  }

  pool[current].keys[pos] = key;
  pool[current].values[pos] = value;
  pool[current].num_keys++;

  unlock_ancestors(ancestors, ancestor_count);
  pthread_rwlock_unlock(&pool[current].lock);
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

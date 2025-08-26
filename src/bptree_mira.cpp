#include "bptree_mira.h"
#include "../../Mira/runtime/libcommon2/include/rvector.h"
#include "bptree_mira_internal.hpp"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define U64_PER_NODE (1024 / sizeof(uint64_t))

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

static inline bptree_node_t *alloc_node() {
  auto &pool = *node_pool2;
  size_t size = pool_size();
  pool_set_size((size + 1) *
                U64_PER_NODE); // pool.resize((size + 1) * U64_PER_NODE);
  return (bptree_node_t *)&pool[size * U64_PER_NODE];
}

static pthread_mutex_t alloc_mutex;

static bptree_node_ref_t bptree_internal_create_node(bool is_leaf) {
  pthread_mutex_lock(&alloc_mutex);
  bptree_node_t *node = alloc_node();
  memset(node, 0, sizeof(bptree_node_t));
  node->is_leaf = is_leaf;
  node->num_keys = 0;
  pthread_rwlock_init(&node->lock, NULL);
  bptree_node_ref_t result = pool_size() - 1;
  pthread_mutex_unlock(&alloc_mutex);
  return result;
}

static void bptree_internal_destroy_node(bptree_node_ref_t node) {
  if (!node)
    return;
  pthread_rwlock_destroy(&get_node(node)->lock);
  // free(node);
}

static bptree_t bptree = {};

void bptree_init() {
  pthread_mutex_init(&alloc_mutex, NULL);
  bptree.root = bptree_internal_create_node(true); // Start with a leaf node
  pthread_rwlock_init(&bptree.root_lock, NULL);
}

static void unlock_ancestors(bptree_node_ref_t *nodes, int count) {
  for (int i = count - 1; i >= 0; i--) {
    pthread_rwlock_unlock(&get_node(nodes[i])->lock);
  }
}

static bool search_node(bptree_node_ref_t node, uint64_t key, int *pos) {
  int left = 0, right = get_node(node)->num_keys - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (get_node(node)->keys[mid] == key) {
      *pos = mid;
      return true;
    }
    if (get_node(node)->keys[mid] < key)
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
  pthread_rwlock_rdlock(&get_node(current)->lock);
  pthread_rwlock_unlock(&bptree.root_lock);

  while (!get_node(current)->is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == get_node(current)->num_keys)
      pos--;

    bptree_node_ref_t next = get_node(current)->children[pos];
    pthread_rwlock_rdlock(&get_node(next)->lock);
    pthread_rwlock_unlock(&get_node(current)->lock);
    current = next;
  }

  int pos;
  bool found = search_node(current, key, &pos);
  if (found && value) {
    *value = get_node(current)->values[pos];
  }
  pthread_rwlock_unlock(&get_node(current)->lock);
  return found;
}

static void split_child(bptree_node_ref_t parent, int index,
                        bptree_node_ref_t child) {
  bptree_node_ref_t new_node =
      bptree_internal_create_node(get_node(child)->is_leaf);
  int mid = (ORDER - 1) / 2;

  get_node(new_node)->num_keys = ORDER - 1 - mid - 1;
  // memcpy(get_node(new_node)->keys, &get_node(child)->keys[mid + 1],
  //        get_node(new_node)->num_keys * sizeof(uint64_t));
  for (int i = 0; i < get_node(new_node)->num_keys; i++) {
    get_node(new_node)->keys[i] = get_node(child)->keys[mid + 1 + i];
  }

  if (get_node(child)->is_leaf) {
    // memcpy(get_node(new_node)->values, &get_node(child)->values[mid + 1],
    //        get_node(new_node)->num_keys * sizeof(uint64_t));
    for (int i = 0; i < get_node(new_node)->num_keys; i++) {
      get_node(new_node)->values[i] = get_node(child)->values[mid + 1 + i];
    }
    get_node(new_node)->next = get_node(child)->next;
    get_node(child)->next = new_node;
    get_node(child)->num_keys = mid + 1;
  } else {
    // memcpy(get_node(new_node)->children, &get_node(child)->children[mid + 1],
    //        (get_node(new_node)->num_keys + 1) * sizeof(bptree_node_t *));
    for (int i = 0; i <= get_node(new_node)->num_keys; i++) {
      get_node(new_node)->children[i] = get_node(child)->children[mid + 1 + i];
    }
    get_node(child)->num_keys = mid;
  }

  for (int i = get_node(parent)->num_keys; i > index; i--) {
    get_node(parent)->keys[i] = get_node(parent)->keys[i - 1];
    get_node(parent)->children[i + 1] = get_node(parent)->children[i];
  }

  get_node(parent)->keys[index] = get_node(child)->keys[mid];
  get_node(parent)->children[index + 1] = new_node;
  get_node(parent)->num_keys++;
}

bool bptree_insert(uint64_t key, uint64_t value) {
  pthread_rwlock_wrlock(&bptree.root_lock);
  bptree_node_ref_t root = bptree.root;

  if (get_node(root)->num_keys == ORDER - 1) {
    bptree_node_ref_t new_root = bptree_internal_create_node(false);
    bptree.root = new_root;
    get_node(new_root)->children[0] = root;
    pthread_rwlock_wrlock(&get_node(new_root)->lock);
    pthread_rwlock_wrlock(&get_node(root)->lock);
    split_child(new_root, 0, root);
    root = new_root;
  } else {
    pthread_rwlock_wrlock(&get_node(root)->lock);
  }
  pthread_rwlock_unlock(&bptree.root_lock);

  bptree_node_ref_t current = root;
  bptree_node_ref_t parent = 0;
  bptree_node_ref_t ancestors[32]; // Stack for lock coupling
  int ancestor_count = 0;

  while (!get_node(current)->is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == get_node(current)->num_keys)
      pos--;

    ancestors[ancestor_count++] = current;
    bptree_node_ref_t child = get_node(current)->children[pos];
    pthread_rwlock_wrlock(&get_node(child)->lock);

    if (get_node(child)->num_keys == ORDER - 1) {
      split_child(current, pos, child);
      if (key > get_node(current)->keys[pos]) {
        pthread_rwlock_unlock(&get_node(child)->lock);
        child = get_node(current)->children[pos + 1];
        pthread_rwlock_wrlock(&get_node(child)->lock);
      }
    }
    current = child;
  }

  int pos;
  if (search_node(current, key, &pos)) {
    // Key already exists, update value
    get_node(current)->values[pos] = value;
    unlock_ancestors(ancestors, ancestor_count);
    pthread_rwlock_unlock(&get_node(current)->lock);
    return false;
  }

  // Insert into leaf node
  for (int i = get_node(current)->num_keys - 1; i >= pos; i--) {
    get_node(current)->keys[i + 1] = get_node(current)->keys[i];
    get_node(current)->values[i + 1] = get_node(current)->values[i];
  }

  get_node(current)->keys[pos] = key;
  get_node(current)->values[pos] = value;
  get_node(current)->num_keys++;

  unlock_ancestors(ancestors, ancestor_count);
  pthread_rwlock_unlock(&get_node(current)->lock);
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

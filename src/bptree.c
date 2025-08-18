#include "../include/bptree.h"
#include <stdlib.h>
#include <string.h>

static bptree_node_t *bptree_internal_create_node(bool is_leaf) {
  bptree_node_t *node = (bptree_node_t *)malloc(sizeof(bptree_node_t));
  if (!node)
    return NULL;

  memset(node, 0, sizeof(bptree_node_t));
  node->is_leaf = is_leaf;
  node->num_keys = 0;
  pthread_rwlock_init(&node->lock, NULL);
  return node;
}

static void bptree_internal_destroy_node(bptree_node_t *node) {
  if (!node)
    return;
  pthread_rwlock_destroy(&node->lock);
  free(node);
}

void bptree_init(bptree_t *tree) {
  tree->root = bptree_internal_create_node(true); // Start with a leaf node
  pthread_rwlock_init(&tree->root_lock, NULL);
}

static void unlock_ancestors(bptree_node_t **nodes, int count) {
  for (int i = count - 1; i >= 0; i--) {
    pthread_rwlock_unlock(&nodes[i]->lock);
  }
}

static bool search_node(bptree_node_t *node, uint64_t key, int *pos) {
  int left = 0, right = node->num_keys - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (node->keys[mid] == key) {
      *pos = mid;
      return true;
    }
    if (node->keys[mid] < key)
      left = mid + 1;
    else
      right = mid - 1;
  }

  *pos = left;
  return false;
}

bool bptree_search(bptree_t *tree, uint64_t key, uint64_t *value) {
  pthread_rwlock_rdlock(&tree->root_lock);
  bptree_node_t *current = tree->root;
  pthread_rwlock_rdlock(&current->lock);
  pthread_rwlock_unlock(&tree->root_lock);

  while (!current->is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == current->num_keys)
      pos--;

    bptree_node_t *next = current->children[pos];
    pthread_rwlock_rdlock(&next->lock);
    pthread_rwlock_unlock(&current->lock);
    current = next;
  }

  int pos;
  bool found = search_node(current, key, &pos);
  if (found && value) {
    *value = current->values[pos];
  }
  pthread_rwlock_unlock(&current->lock);
  return found;
}

static void split_child(bptree_node_t *parent, int index,
                        bptree_node_t *child) {
  bptree_node_t *new_node = bptree_internal_create_node(child->is_leaf);
  int mid = (ORDER - 1) / 2;

  new_node->num_keys = ORDER - 1 - mid - 1;
  memcpy(new_node->keys, &child->keys[mid + 1],
         new_node->num_keys * sizeof(uint64_t));

  if (child->is_leaf) {
    memcpy(new_node->values, &child->values[mid + 1],
           new_node->num_keys * sizeof(uint64_t));
    new_node->next = child->next;
    child->next = new_node;
    child->num_keys = mid + 1;
  } else {
    memcpy(new_node->children, &child->children[mid + 1],
           (new_node->num_keys + 1) * sizeof(bptree_node_t *));
    child->num_keys = mid;
  }

  for (int i = parent->num_keys; i > index; i--) {
    parent->keys[i] = parent->keys[i - 1];
    parent->children[i + 1] = parent->children[i];
  }

  parent->keys[index] = child->keys[mid];
  parent->children[index + 1] = new_node;
  parent->num_keys++;
}

bool bptree_insert(bptree_t *tree, uint64_t key, uint64_t value) {
  pthread_rwlock_wrlock(&tree->root_lock);
  bptree_node_t *root = tree->root;

  if (root->num_keys == ORDER - 1) {
    bptree_node_t *new_root = bptree_internal_create_node(false);
    tree->root = new_root;
    new_root->children[0] = root;
    pthread_rwlock_wrlock(&new_root->lock);
    pthread_rwlock_wrlock(&root->lock);
    split_child(new_root, 0, root);
    root = new_root;
  } else {
    pthread_rwlock_wrlock(&root->lock);
  }
  pthread_rwlock_unlock(&tree->root_lock);

  bptree_node_t *current = root;
  bptree_node_t *parent = NULL;
  bptree_node_t *ancestors[32]; // Stack for lock coupling
  int ancestor_count = 0;

  while (!current->is_leaf) {
    int pos;
    search_node(current, key, &pos);
    if (pos == current->num_keys)
      pos--;

    ancestors[ancestor_count++] = current;
    bptree_node_t *child = current->children[pos];
    pthread_rwlock_wrlock(&child->lock);

    if (child->num_keys == ORDER - 1) {
      split_child(current, pos, child);
      if (key > current->keys[pos]) {
        pthread_rwlock_unlock(&child->lock);
        child = current->children[pos + 1];
        pthread_rwlock_wrlock(&child->lock);
      }
    }
    current = child;
  }

  int pos;
  if (search_node(current, key, &pos)) {
    // Key already exists, update value
    current->values[pos] = value;
    unlock_ancestors(ancestors, ancestor_count);
    pthread_rwlock_unlock(&current->lock);
    return false;
  }

  // Insert into leaf node
  for (int i = current->num_keys - 1; i >= pos; i--) {
    current->keys[i + 1] = current->keys[i];
    current->values[i + 1] = current->values[i];
  }

  current->keys[pos] = key;
  current->values[pos] = value;
  current->num_keys++;

  unlock_ancestors(ancestors, ancestor_count);
  pthread_rwlock_unlock(&current->lock);
  return true;
}

void bptree_destroy(bptree_t *tree) {}

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

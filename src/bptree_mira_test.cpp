#include "bptree_mira.h"
#include "bptree_mira_internal.hpp"
#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define NUM_THREADS 8
#define NUM_OPERATIONS (1000000 / NUM_THREADS)

typedef struct {
  bptree_t *tree;
  int thread_id;
  uint64_t ops_completed;
  struct timeval start_time;
  struct timeval end_time;
} thread_data_t;

double get_time_diff_ms(struct timeval start, struct timeval end) {
  return (end.tv_sec - start.tv_sec) * 1000.0 +
         (end.tv_usec - start.tv_usec) / 1000.0;
}

void *worker_thread(void *arg) {
  thread_data_t *data = (thread_data_t *)arg;
  bptree_t *tree = data->tree;
  unsigned int seed = data->thread_id; // Different seed for each thread

  gettimeofday(&data->start_time, NULL);

  for (int i = 0; i < NUM_OPERATIONS; i++) {
    uint64_t key = rand_r(&seed) % (NUM_OPERATIONS * 10);
    uint64_t value = rand_r(&seed);

    int op = rand_r(&seed) % 100;
    // Operations on populated tree:
    // - 80% reads (realistic for most applications)
    // - 20% writes (inserts/updates)
    if (op < 80) { // 80% searches
      uint64_t found_value;
      // Mix of existing and non-existing keys
      uint64_t search_key = (rand_r(&seed) % 2 == 0)
                                ?
                                // Search for potentially existing key
                                rand_r(&seed)
                                :
                                // Search for likely non-existing key
                                (NUM_OPERATIONS * 10) + rand_r(&seed) % 1000000;
      bptree_search(search_key, &found_value);
    } else { // 20% inserts/updates
      bptree_insert(key, value);
    }
    data->ops_completed++;
  }

  gettimeofday(&data->end_time, NULL);
  return NULL;
}

void populate_tree(size_t num_items) {
  printf("Populating tree with %zu initial items...\n", num_items);
  struct timeval start, end;
  gettimeofday(&start, NULL);

  unsigned int seed = 12345; // Fixed seed for reproducibility
  for (size_t i = 0; i < num_items; i++) {
    uint64_t key = rand_r(&seed);
    uint64_t value = key * 2; // Deterministic value based on key
    bptree_insert(key, value);
  }

  gettimeofday(&end, NULL);
  double time_taken = get_time_diff_ms(start, end);
  printf("Tree populated in %.2f ms\n\n", time_taken);
}

int main() {
  ext_init();
  // bptree_t tree;
  bptree_init();

  // Populate tree with initial data (about 1M items)
  populate_tree(1000);

  pthread_t threads[NUM_THREADS];
  thread_data_t thread_data[NUM_THREADS];

  // Create threads
  for (int i = 0; i < NUM_THREADS; i++) {
    // thread_data[i].tree = &tree;
    thread_data[i].thread_id = i;
    thread_data[i].ops_completed = 0;
    pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]);
  }

  // Wait for threads to complete
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  // Calculate statistics
  double total_time = 0;
  uint64_t total_ops = 0;

  for (int i = 0; i < NUM_THREADS; i++) {
    double thread_time =
        get_time_diff_ms(thread_data[i].start_time, thread_data[i].end_time);
    total_time += thread_time;
    total_ops += thread_data[i].ops_completed;

    printf("Thread %d completed %lu operations in %.2f ms\n", i,
           thread_data[i].ops_completed, thread_time);
  }

  double avg_time = total_time / NUM_THREADS;
  double ops_per_second = (total_ops * 1000.0) / avg_time;

  printf("\nAverage time per thread: %.2f ms\n", avg_time);
  printf("Total operations: %lu\n", total_ops);
  printf("Operations per second: %.2f\n", ops_per_second);

  // Verify tree correctness
  printf("\nVerifying tree consistency...\n");
  uint64_t test_key = 42;
  uint64_t test_value = 123456;

  bptree_insert(test_key, test_value);
  uint64_t found_value;
  if (bptree_search(test_key, &found_value) &&
      found_value == test_value) {
    printf("Tree verification passed!\n");
  } else {
    printf("Tree verification failed!\n");
  }

  bptree_destroy();
  return 0;
}

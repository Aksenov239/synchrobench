#include <getopt.h>
#include <signal.h>
#include <sys/time.h>
#include <limits>
#include <vector>
#include <fstream>

#include "cmdline.hpp"
#include "adapter.h"
#include "atomic_ops.h"

#define DEFAULT_DURATION                1000
#define DEFAULT_INITIAL                 256
#define DEFAULT_NB_THREADS              1
#define DEFAULT_RANGE                   0x7FFFFFFF
#define DEFAULT_SEED                    0
#define DEFAULT_UPDATE                  20
#define DEFAULT_ELASTICITY              4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               1

//#define THROTTLE_NUM  1000
//#define THROTTLE_TIME 10000
//#define THROTTLE_MAINTENANCE

//typedef unsigned long long uint64_t;

volatile AO_t stop;
unsigned int global_seed;

typedef struct barrier {
  pthread_cond_t complete;
  pthread_mutex_t mutex;
  int count;
  int crossing;
} barrier_t;

void barrier_init(barrier_t* b, int n) {
  pthread_cond_init(&b->complete, NULL);
  pthread_mutex_init(&b->mutex, NULL);
  b->count = n;
  b->crossing = 0;
}

void barrier_cross(barrier_t* b) {
  pthread_mutex_lock(&b->mutex);
  /* One more thread through */
  b->crossing++;
  /* If not all here, wait */
  if (b->crossing < b->count) {
    pthread_cond_wait(&b->complete, &b->mutex);
  } else {
    pthread_cond_broadcast(&b->complete);
    /* Reset for next time */
    b->crossing = 0;
  }
  pthread_mutex_unlock(&b->mutex);
}

class thread_data {
public:
  int first;
  int range;
  int update;
  int alternate;
  int effective;
  int id;
  unsigned long nb_add;
  unsigned long nb_added;
  unsigned long nb_remove;
  unsigned long nb_removed;
  unsigned long nb_contains;
  unsigned long nb_found;
  unsigned int seed;
  barrier_t* barrier;
  ds_adapter<int, void*>* tree;

  int iterations;
  std::vector<uint64_t> contains_times;
  std::vector<uint64_t> add_times;
  std::vector<uint64_t> remove_times;
};

inline uint64_t getticks() {
  unsigned int lo, hi;
  // RDTSC copies contents of 64-bit TSC into EDX:EAX
  __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
  return (unsigned long long)hi << 32 | lo;
}


/* 
 * Returns a pseudo-random value in [1;range).
 * Depending on the symbolic constant RAND_MAX>=32767 defined in stdlib.h,
 * the granularity of rand() could be lower-bounded by the 32767^th which might 
 * be too high for given values of range and initial.
 */
inline long rand_range(long r) {
  int m = RAND_MAX;
  int d, v = 0;

  do {
    d = (m > r ? r : m);    
    v += 1 + (int)(d * ((double)rand()/((double)(m)+1.0)));
    r -= m;
  } while (r > 0);
  return v;
}

/* Re-entrant version of rand_range(r) */
inline long rand_range_re(unsigned int* seed, long r) {
  int m = RAND_MAX;
  int d, v = 0;

  do {
    d = (m > r ? r : m);    
    v += 1 + (int)(d * ((double)rand_r(seed)/((double)(m)+1.0)));
    r -= m;
  } while (r > 0);
  return v;
}

void* test(void* data) {
  int last = -1;
  int val = 0;

  thread_data* d = (thread_data*) data;
  int iterations = d->iterations;
  auto tree = d->tree;
  tree->initThread(d->id);
  int unext = (rand_range_re(&d->seed, 100) - 1 < d->update);

  /* Wait on barrier */
  barrier_cross(d->barrier);
  
  /* Is the first op an update? */

  uint64_t start;

  int it = 0;
  while ((stop == 0 && iterations == -1) || it < iterations) {
    if (unext) { // update
      if (last < 0) { // add
        val = rand_range_re(&d->seed, d->range);
        assert(val > 0);
        if (iterations != -1) {
          start = getticks();
        }
        void* res = tree->insertIfAbsent(d->id, val, (void*) 1);
        if (iterations != -1) {
          d->add_times[d->nb_add] = getticks() - start;
        }
        if (res == tree->getNoValue()) {
          last = val;
          d->nb_added++;
        }         
        d->nb_add++;
      } else { // remove
        // Random computation only in non-alternated cases 
        val = rand_range_re(&d->seed, d->range);
        if (iterations != -1) {
          start = getticks();
        }
        // Remove one random value 
        void* res = tree->erase(d->id, val);
        if (iterations != -1) {
          d->remove_times[d->nb_remove] = getticks() - start;
        }
        if (res != tree->getNoValue()) {
          // Repeat until successful, to avoid size variations 
          last = -1;
          d->nb_removed++;
        }
        d->nb_remove++;
      }
    } else { // read
      if (d->alternate) {
        if (d->update == 0) {
          if (last < 0) {
            val = d->first;
            last = val;
          } else { // last >= 0
            val = rand_range_re(&d->seed, d->range);
            last = -1;
          }
        } else { // update != 0
          if (last < 0) {
            val = rand_range_re(&d->seed, d->range);
            //last = val;
          } else {
            val = last;
          }
        }
      }  else {
        val = rand_range_re(&d->seed, d->range);
      }
      
      if (iterations != -1) {
        start = getticks();
      }
      bool res = tree->contains(d->id, val);
      if (iterations != -1) {
        d->contains_times[d->nb_contains] = getticks() - start;
      }
      if (res) {
        d->nb_found++;
      }
      d->nb_contains++;
    }
    
    /* Is the next op an update? */
    if (d->effective) { // a failed remove/add is a read-only tx
      unext = ((100 * (d->nb_added + d->nb_removed))
         < (d->update * (d->nb_add + d->nb_remove + d->nb_contains)));
    } else { // remove/add (even failed) is considered as an update
      unext = ((rand_range_re(&d->seed, 100) - 1) < d->update);
    }
    it++;
  }
  
  tree->deinitThread(d->id);

  return NULL;
}

int main(int argc, char **argv) {
  int i, c, size;
  int last = 0; 
  int val = 0;
  unsigned long reads, effreads, updates, effupds, aborts, aborts_locked_read, 
    aborts_locked_write, aborts_validate_read, aborts_validate_write, 
    aborts_validate_commit, aborts_invalid_memory, max_retries;
  thread_data* data;
  pthread_t* threads;
  pthread_attr_t attr;
  barrier_t barrier;
  struct timeval start, end;
  struct timespec timeout;

  deepsea::cmdline::set(argc, argv);
  int duration = deepsea::cmdline::parse_or_default_int("d", DEFAULT_DURATION);
  int initial = deepsea::cmdline::parse_or_default_int("init", DEFAULT_INITIAL);
  int nb_threads = deepsea::cmdline::parse_or_default_int("threads", DEFAULT_NB_THREADS);
  int range = deepsea::cmdline::parse_or_default_int("range", DEFAULT_RANGE);
  int seed = deepsea::cmdline::parse_or_default_int("seed", DEFAULT_SEED);
  int update = deepsea::cmdline::parse_or_default_int("update", DEFAULT_UPDATE);
  bool alternate = deepsea::cmdline::parse_or_default_bool("alternate", DEFAULT_ALTERNATE);
  bool effective = deepsea::cmdline::parse_or_default_bool("effective", DEFAULT_EFFECTIVE);
  int iterations = deepsea::cmdline::parse_or_default_int("iterations", -1);
  sigset_t block_set;

  assert(duration >= 0);
  assert(initial >= 0);
  assert(nb_threads > 0);
  assert(range > 0 && range >= initial);
  assert(update >= 0 && update <= 100);

  printf("Set type     : BST\n");
  printf("Duration     : %d\n", duration);
  printf("Initial size : %d\n", initial);
  printf("Nb threads   : %d\n", nb_threads);
  printf("Value range  : %d\n", range);
  printf("Seed         : %d\n", seed);
  printf("Update rate  : %d\n", update);
  printf("Alternate    : %d\n", alternate);
  printf("Effective    : %d\n", effective);
  printf("Iterations   : %d\n", iterations);
  printf("Type sizes   : int=%d/long=%d/ptr=%d/word=%d\n",
    (int)sizeof(int),
    (int)sizeof(long),
    (int)sizeof(void *),
    (int)sizeof(uintptr_t)
  );
		
  timeout.tv_sec = duration / 1000;
  timeout.tv_nsec = (duration % 1000) * 1000000;

  const int KEY_NEG_INFTY = std::numeric_limits<int>::min();
  const int unused1 = 0;
  void* const unused2 = NULL;
  RandomFNV1A* const unused3 = NULL;

  auto tree = new ds_adapter<int, void*>(nb_threads + 1, KEY_NEG_INFTY, unused1, unused2, unused3);

  data = (thread_data*) malloc(nb_threads * sizeof(thread_data));
  threads = (pthread_t*) malloc(nb_threads * sizeof(pthread_t));
		
  if (seed == 0) {
    srand((int)time(0));
  } else {
    srand(seed);
  }
		
  /* Populate set */
  tree->initThread(0);
  for (int i = 0; i < initial; ) {
    val = rand_range_re(&global_seed, range);
    if (tree->insertIfAbsent(0, val, (void*) 1) == tree->getNoValue()) {
	i++;
    }
  }

  printf("Set size (TENTATIVE) : %d\n", initial);
		
  /* Access set from all threads */
  barrier_init(&barrier, nb_threads + 1);
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (i = 0; i < nb_threads; i++) {
    printf("Creating thread %d\n", i);
    data[i].first = last;
    data[i].range = range;
    data[i].update = update;
    data[i].alternate = alternate;
    data[i].effective = effective;
    data[i].nb_add = 0;
    data[i].nb_added = 0;
    data[i].nb_remove = 0;
    data[i].nb_removed = 0;
    data[i].nb_contains = 0;
    data[i].nb_found = 0;
    data[i].barrier = &barrier;
    data[i].id = i + 1;
    data[i].seed = rand();
    data[i].tree = tree;
    data[i].iterations = iterations;
    if (iterations != -1) {
      data[i].contains_times.assign(iterations, 0);
      data[i].add_times.assign(iterations, 0);
      data[i].remove_times.assign(iterations, 0);
    }

    if (pthread_create(&threads[i], &attr, test, (void*)(&data[i])) != 0) {
	fprintf(stderr, "Error creating thread\n");
	exit(1);
    }
  }
  pthread_attr_destroy(&attr);
		
  /* Start threads */
  barrier_cross(&barrier);
		
  printf("STARTING...\n");
  gettimeofday(&start, NULL);
  if (duration > 0) {
    nanosleep(&timeout, NULL);
  } else {
    sigemptyset(&block_set);
    sigsuspend(&block_set);
  }

  AO_store_full(&stop, 1);

  gettimeofday(&end, NULL);
  printf("STOPPING...\n");

  /* Wait for thread completion */
  for (i = 0; i < nb_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
	fprintf(stderr, "Error waiting for thread completion\n");
	exit(1);
    }
  }

  if (iterations != -1) {
    char name[80];
    sprintf(name, "logs/add-%d-%d-%d.txt", nb_threads, update, range);
    std::ofstream add_file(name);
    for (int i = 0; i < data[0].nb_add; i++) {
      add_file << data[0].add_times[i] << std::endl;
    }
    add_file.close();

    sprintf(name, "logs/remove-%d-%d-%d.txt", nb_threads, update, range);
    std::ofstream remove_file(name);
    for (int i = 0; i < data[0].nb_remove; i++) {
      remove_file << data[0].remove_times[i] << std::endl;
    }
    remove_file.close();

    sprintf(name, "logs/contain-%d-%d-%d.txt", nb_threads, update, range);
    std::ofstream contains_file(name);
    for (int i = 0; i < data[0].nb_contains; i++) {
      contains_file << data[0].contains_times[i] << std::endl;
    }
    contains_file.close();
  }

  duration = (end.tv_sec * 1000 + end.tv_usec / 1000) - 
    (start.tv_sec * 1000 + start.tv_usec / 1000);
  reads = 0;
  effreads = 0;
  updates = 0;
  effupds = 0;
  max_retries = 0;
  for (i = 0; i < nb_threads; i++) {
    printf("Thread %d\n", i);
    printf("  #add        : %lu\n", data[i].nb_add);
    printf("    #added    : %lu\n", data[i].nb_added);
    printf("  #remove     : %lu\n", data[i].nb_remove);
    printf("    #removed  : %lu\n", data[i].nb_removed);
    printf("  #contains   : %lu\n", data[i].nb_contains);
    printf("  #found      : %lu\n", data[i].nb_found);
    reads += data[i].nb_contains;
    effreads += data[i].nb_contains + 
	(data[i].nb_add - data[i].nb_added) + 
	(data[i].nb_remove - data[i].nb_removed); 
    updates += (data[i].nb_add + data[i].nb_remove);
    effupds += data[i].nb_removed + data[i].nb_added; 
//    size += data[i].nb_added - data[i].nb_removed;
  }

    //printf("Set size      : %d (expected: %d)\n", sl_set_size(set), size);
  printf("Duration      : %d (ms)\n", duration);
  printf("#txs          : %lu (%f / s)\n", reads + updates, 
	   (reads + updates) * 1000.0 / duration);
		
  printf("#read txs     : ");
  if (effective) {
    printf("%lu (%f / s)\n", effreads, effreads * 1000.0 / duration);
    printf("  #contains   : %lu (%f / s)\n", reads, reads * 1000.0 / 
	     duration);
  } else printf("%lu (%f / s)\n", reads, reads * 1000.0 / duration);
		
  printf("#eff. upd rate: %f \n", 100.0 * effupds / (effupds + effreads));
		
  printf("#update txs   : ");
  if (effective) {
    printf("%lu (%f / s)\n", effupds, effupds * 1000.0 / duration);
    printf("  #upd trials : %lu (%f / s)\n", updates, updates * 1000.0 / 
	     duration);
  } else {
    printf("%lu (%f / s)\n", updates, updates * 1000.0 / duration);
  }

  tree->deinitThread(0);
  /* Delete set */
  delete tree;
		
  free(threads);
  free(data);
		
  return 0;
}

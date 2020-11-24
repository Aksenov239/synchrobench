/*
 * File:
 *   lazy.c
 * Author(s):
 *   Vincent Gramoli <vincent.gramoli@epfl.ch>
 * Description:
 *   Lazy linked list implementation of an integer set based on Heller et al. algorithm
 *   "A Lazy Concurrent List-Based Set Algorithm"
 *   S. Heller, M. Herlihy, V. Luchangco, M. Moir, W.N. Scherer III, N. Shavit
 *   p.3-16, OPODIS 2005
 *
 * Copyright (c) 2009-2010.
 *
 * lazy.c is part of Synchrobench
 * 
 * Synchrobench is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include <cassert>
#include <iostream>
#include <atomic>
#include "lazy.h"

inline int is_marked_ref(long i) {
  return (int) (i &= LONG_MIN+1);
}

inline long unset_mark(long i) {
  i &= LONG_MAX-1;
  return i;
}

inline long set_mark(long i) {
  i = unset_mark(i);
  i += 1;
  return i;
}

inline node_l_t *get_unmarked_ref(node_l_t *n) {
  return (node_l_t *) unset_mark((long) n);
}

inline node_l_t *get_marked_ref(node_l_t *n) {
  return (node_l_t *) set_mark((long) n);
}

inline int lock_ref(node_l_t *node, node_l_t *next) {
  __asm volatile ("mfence" ::: "memory");
  node_l_t* n = node->next;
  if (is_marked_ref((long) n) || n != next) {
    return 0;
  }
  LOCK(&node->lock);
  n = node->next;
  if (is_marked_ref((long) n) || n != next){
    UNLOCK(&node->lock);
    return 0;
  }
  return 1;
}

inline int lock_val(node_l_t *node, val_t val) {
  __asm volatile ("mfence" ::: "memory");
  node_l_t* n = node->next;
  if (is_marked_ref((long) n) || n->val != val) {
    return 0;
  }
  LOCK(&node->lock);
  n = node->next;
  if (is_marked_ref((long) n) || n->val != val){
    UNLOCK(&node->lock);
    return 0;
  }
  return 1;
}

int parse_find(intset_l_t *set, val_t val) {
  node_l_t *curr;
  curr = set->head;
  while (curr->val < val)
    curr = get_unmarked_ref(curr->next);
  return ((curr->val == val) && !is_marked_ref((long) curr->next));
}

int parse_insert(intset_l_t *set, val_t val) {
  node_l_t *curr, *pred, *newnode;
//  std::cerr << "insert " << val << std::endl;
  
  while (1) {         
    pred = set->head;
    curr = get_unmarked_ref(pred->next);
    while (curr->val < val) {
      pred = curr;
      curr = get_unmarked_ref(curr->next);
    }
    if (curr->val == val) {
      return 0;
    }
    if (!lock_ref(pred, curr)) {
//      std::cerr << "Fuck insert!\n";
      continue;
    }

//    if (!is_marked_ref((long) (pred->next)) && pred->next == curr) {} else {std::cerr << "Fuck!\n"; }

    newnode = new_node_l(val, curr, 0);
    pred->next = newnode;
    UNLOCK(&pred->lock);
    return 1;
  }
}

/*
 * Logically remove an element by setting a mark bit to 1 
 * before removing it physically.
 *
 * NB. it is not safe to free the element after physical deletion as a 
 * pre-empted find operation may currently be parsing the element.
 * TODO: must implement a stop-the-world garbage collector to correctly 
 * free the memory.
 */
int parse_delete(intset_l_t *set, val_t val) {
  node_l_t *pred, *curr, *next;

//  std::cerr << "delete " << val << std::endl;

  while (1) {
    pred = set->head;
    curr = get_unmarked_ref(pred->next);
    while (curr->val < val) {
      pred = curr;
      curr = get_unmarked_ref(curr->next);
    }
    if (curr->val != val) {
      return 0;
    }

    next = get_unmarked_ref(curr->next);

    if (!lock_val(pred, val)) {
//      std::cerr << "Fuck delete!\n";
      continue;
    }
    curr = pred->next;

    if (!lock_ref(curr, next)) {
//      std::cerr << "Fuck delete!\n";
      UNLOCK(&pred->lock);
      continue;
    }

//    if (!is_marked_ref((long) (pred->next)) && !is_marked_ref((long) (curr->next)) && pred->next == curr && curr->next == next) {} else {std::cerr << "Fuck!\n"; }

    curr->next = get_marked_ref(next);
    pred->next = next;
    UNLOCK(&curr->lock);
    UNLOCK(&pred->lock);
    return 1;
  }
}

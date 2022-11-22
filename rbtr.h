/*

MIT License

Copyright (c) 2022 Alexander Zazhigin mykeich@yandex.ru

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


/*
 * Copied from http://oopweb.com/Algorithms/Documents/Sman/VolumeFrames.html?/Algorithms/Documents/Sman/Volume/RedBlackTrees_files/s_rbt.htm
 *
 * Disclosure from the author's main page:
 * (http://oopweb.com/Algorithms/Documents/Sman/VolumeFrames.html?/Algorithms/Documents/Sman/Volume/RedBlackTrees_files/s_rbt.htm)
 *
 *     Source code when part of a software project may be used freely
 *     without reference to the author.
 *
 */

#ifndef RBT_H
#define RBT_H

typedef enum {
    RBT_STATUS_OK, RBT_STATUS_MEM_EXHAUSTED, RBT_STATUS_DUPLICATE_KEY, RBT_STATUS_KEY_NOT_FOUND
} RbtStatus;

typedef void *RbtIterator;
typedef void *RbtHandle;

RbtHandle rbtNew(int (*compare)(void *a, void *b));
// create red-black tree
// parameters:
//     compare  pointer to function that compares keys
//              return 0   if a == b
//              return < 0 if a < b
//              return > 0 if a > b
// returns:
//     handle   use handle in calls to rbt functions

void rbtDelete(RbtHandle h);
// destroy red-black tree

RbtStatus rbtInsert(RbtHandle h, void *key, void *val, void **out);
// insert key/value pair

RbtStatus rbtErase(RbtHandle h, RbtIterator i);
// delete node in tree associated with iterator
// this function does not free the key/value pointers

RbtIterator rbtNext(RbtHandle h, RbtIterator i);
// return ++i

RbtIterator rbtBegin(RbtHandle h);
// return pointer to first node

RbtIterator rbtEnd(RbtHandle h);
// return pointer to one past last node

void rbtKeyValue(RbtHandle h, RbtIterator i, void **key, void **value);
// returns key/value pair associated with iterator

void rbtUpdate(RbtHandle h, RbtIterator it, void *zdbval);
// replace val in node

RbtIterator rbtFind(RbtHandle h, void *key);
// returns iterator associated with key


RbtIterator rbtScan(RbtHandle h, void *key);

#endif

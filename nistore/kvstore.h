// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/kvstore.h:
 *   Simple historied key-value store
 *
 **********************************************************************/

#ifndef _KV_STORE_H_
#define _KV_STORE_H_

#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iostream>
#include <list>

namespace nistore {
using namespace std;

class KVStore
{

public:
    KVStore();
    ~KVStore();

    bool get(const string &key, string &value);
    bool put(const string &key, const string &value);
    bool remove(const string &key, string &value);

private:
    /* Global store which keep key -> (timestamp, value) list. */
    unordered_map<string, list<string>> store;
};

} // namespace nistore

#endif  /* _KV_STORE_H_ */

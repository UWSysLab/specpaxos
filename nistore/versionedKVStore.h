// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/versionedKVStore.h:
 *   Simple versioned key-value store
 *
 **********************************************************************/

#ifndef _NI_VERSIONED_KV_STORE_H_
#define _NI_VERSIONED_KV_STORE_H_

#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iostream>
#include <list>

namespace nistore {
using namespace std;

class VersionedKVStore
{

public:
    VersionedKVStore();
    ~VersionedKVStore();

    bool get(const string &key, pair<uint64_t, string> &value);
    bool get(const string &key, uint64_t timestamp, string &value);
    bool put(const string &key, const string &value, uint64_t timestamp);
    bool remove(const string &key, pair<uint64_t, string> &value);

private:
    /* Global store which keep key -> (timestamp, value) list. */
    unordered_map<string, list<pair<uint64_t, string> > > store;
};

} // namespace nistore

#endif  /* _NI_VERSIONED_KV_STORE_H_ */

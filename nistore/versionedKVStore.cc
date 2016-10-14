// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/versionedKVStore.cc:
 *   Simple versioned key-value store
 *
 **********************************************************************/

#include "nistore/versionedKVStore.h"
#include "lib/assert.h"
#include "lib/message.h"

namespace nistore {
using namespace std;

VersionedKVStore::VersionedKVStore() { }
    
VersionedKVStore::~VersionedKVStore() { }
    

/* Returns the most recent value and timestamp for given key.
 * Error if key does not exist. */
bool 
VersionedKVStore::get(const string &key, pair<uint64_t, string> &value)
{
    // check for existence of key in store
    if (store.find(key) == store.end()) {
        return false;
    } else {
	value = store[key].front();
        return true;
    }
}
    
/* Returns the value valid at given timestamp.
 * Error if key did not exist at the timestamp. */
bool
VersionedKVStore::get(const string &key, uint64_t timestamp, string &value)
{
    // check for existence of key in store
    if (store.find(key) == store.end()) {
        return false;
    } else {
        list<pair<uint64_t, string> >::iterator it = store[key].begin();
        while (it != store[key].end()) {
            if ( timestamp >= (*it).first ) {
                value = (*it).second;
                return true;
            }
            it++;
        }
    }
    return false;
}

bool
VersionedKVStore::put(const string &key, const string &value, uint64_t timestamp)
{
    // Key does not exist. Create a list and an entry.
    if (store.find(key) == store.end()) {
        list<pair<uint64_t, string> > l;
        l.push_front(make_pair(timestamp, value));
        store[key] = l;
        return true;
    } 

    // Key exists, add it to list of values if newer timestamp.
    if (timestamp > store[key].front().first) {
        store[key].push_front(make_pair(timestamp, value));
        return true;
    } else {
        // newer version exists, insert older version
        list<pair<uint64_t, string> >::iterator it = store[key].begin();
        while (it != store[key].end()) {
            if ( timestamp > (*it).first ) {
                store[key].insert(it, make_pair(timestamp, value));
                return true;
            }
            it++;
        }
        return true;
    }

    return false;
}

/* Delete the latest version of this key. */
bool
VersionedKVStore::remove(const string &key, pair<uint64_t, string> &value)
{
    if (store.find(key) == store.end()) {
        return false;
    } 

    value = store[key].front();
    store[key].pop_front();
    return true;
}

} // namespace nistore

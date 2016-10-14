// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/occstore.cc:
 *   Key-value store with support for OCC
 *
 **********************************************************************/
#include "nistore/occstore.h"
#include "lib/assert.h"
#include "lib/message.h"

namespace nistore {

using namespace std;

bool
OCCStore::Transaction::operator==(const Transaction &t)
{
    return id == t.id;
}

OCCStore::OCCStore() :
   store() { }
OCCStore::~OCCStore() { }

set<string>
OCCStore::getPreparedWrites()
{
    // gather up the set of all writes that we are currently prepared for
    set<string> writes;
    for (auto &t : prepared) {
        for (auto &write : t.second.writeSet) {
            writes.insert(write.first);
        }
    }
    return writes;
}

set<string>
OCCStore::getPreparedReadWrites()
{
    // gather up the set of all writes that we are currently prepared for
    set<string> readwrites;
    for (auto &t : prepared) {
        for (auto &write : t.second.writeSet) {
            readwrites.insert(write.first);
        }
        for (auto &read : t.second.readSet) {
            readwrites.insert(read.first);
        }
    }
    return readwrites;
}

// Gets the running transaction. Creates one if there isn't one for this client
OCCStore::Transaction&
OCCStore::getTxn(uint64_t id)
{
    if (running.find(id) == running.end()) {
        Transaction txn(id);
        running[id] = txn;
    } else {
        ASSERT(running[id].id == id);
    }

    return running[id];
}

OCCStore::Transaction
OCCStore::getRetiredTxn(opnum_t op, uint64_t id, RetiredState state)
{
    pair<opnum_t,RetiredTxn> t = retired.back();
    RetiredTxn rTxn = t.second;

    ASSERT(t.first == op);
    ASSERT(rTxn.txn.id == id);
    ASSERT(rTxn.state == state);

    retired.pop_back();
    return rTxn.txn;
}

    
void
OCCStore::begin(uint64_t id)
{
    Debug("[%" PRIu64 "] BEGIN", id);
    Transaction txn(id);
    running[id] = txn;
}

void
OCCStore::unbegin(uint64_t id)
{
    Debug("[%" PRIu64 "] UNDO BEGIN", id);
    if (running.find(id) != running.end()) {
        running.erase(id);
    }
}

int
OCCStore::get(uint64_t id, const string &key, string &value)
{
    Debug("[%" PRIu64 "] GET %s", id, key.c_str());

    Transaction &txn = getTxn(id);
    bool ret;

    // Read your own writes, check the write set first
    if (txn.writeSet.find(key) != txn.writeSet.end()) {
        // Don't need to add this to the read set
        // because it can't conflict
        value = txn.writeSet[key].back();
        return 0;
    }

    // Consistent reads, check the read set
    if (txn.readSet.find(key) != txn.readSet.end()) {
        string val;
        // if we've read this before, read from
        // the same timestamp
        ret = store.get(key,txn.readSet[key].first, val);
        ASSERT(ret);
        // increment the read counter
        txn.readSet[key].second++;
        value = val;
        return 0;
    }

    // Otherwise get latest from store
    pair<uint64_t, string> val;
    ret = store.get(key, val);
    if (ret) {
        txn.readSet[key] = make_pair(val.first, 1);
        value = val.second;
        return 0;
    } else {
        return -1;
    }
}

void
OCCStore::unget(uint64_t id, const string &key)
{
    Debug("[%" PRIu64 "] UNDO GET %s", id, key.c_str());

    Transaction &txn = getTxn(id);

    // if we find this in the read set
    if (txn.readSet.find(key) != txn.readSet.end()) {
        pair<uint64_t,int> &read = txn.readSet[key];
        if (read.second > 1) {
            read.second--;
        } else {
            txn.readSet.erase(key);
        }
    }
    // we may not find the read if it was a self-read, etc.
}

int
OCCStore::put(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] PUT %s %s", id, key.c_str(), value.c_str());
    Transaction &txn = getTxn(id);

    // record the write
    txn.writeSet[key].push_back(value);
    return 0;
}

void
OCCStore::unput(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] UNDO PUT %s %s", id, key.c_str(), value.c_str());
    Transaction &txn = getTxn(id);

    ASSERT(txn.writeSet.find(key) != txn.writeSet.end());
    ASSERT(!txn.writeSet[key].empty());

    ASSERT(txn.writeSet[key].back() == value);
    txn.writeSet[key].pop_back();
}

int
OCCStore::prepare(uint64_t id, opnum_t op)
{    
    Debug("[%" PRIu64 "] START PREPARE", id);

    if (running.find(id) == running.end()) {
        Notice("Could not find transaction [%" PRIu64 "] to prepare", id);
        return -1;
    } else {
        Transaction txn = running[id];

        // do OCC checks
        set<string> pWrites = getPreparedWrites();
        set<string> pRW = getPreparedReadWrites();

        // check for conflicts with the read set
        for (auto read : txn.readSet) {
            pair<uint64_t, string> cur;
	    bool ret = store.get(read.first, cur);

	    ASSERT(ret);

            // if this key has been written since we read it, abort
            if (cur.first != read.second.first) {
                Debug("[%" PRIu64 "] ABORT rw conflict key:%s",
                        id, read.first.c_str());

                abortTxn(id, op);
                return -1;
            }

            //if there is a pending write for this key, abort
            if (pWrites.find(read.first) != pWrites.end()) {
                Debug("[%" PRIu64 "] ABORT rw conflict w/ prepared key:%s",
                        id, read.first.c_str());
                abortTxn(id, op);
                return -1;
            }
        }

        // check for conflicts with the write set
        for (auto write : txn.writeSet) {
            //if there is a pending read or write for this key, abort
            if (pRW.find(write.first) != pRW.end()) {
                Debug("[%" PRIu64 "] ABORT ww conflict w/ prepared key:%s", 
                        id, write.first.c_str());
                abortTxn(id, op);
                return -1;
            }
        }

        // Otherwise, prepare this transaction for commit
        running.erase(id);
        prepared[id] = txn;
        Debug("[%" PRIu64 "] PREPARED TO COMMIT", id);
        return 0;
    }
}

void
OCCStore::unprepare(uint64_t id, opnum_t op)
{
    if (prepared.find(id) != prepared.end()) {
        // revert back to running
        running[id] = prepared[id];
        prepared.erase(id);
    } else {
        // this transaction was aborted during prepare
        // revert back to running
        Transaction txn = getRetiredTxn(op, id, ABORTED_RUNNING);
        running[id] = txn;
    }
}

void
OCCStore::commit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    Debug("[%" PRIu64 "] COMMIT", id);
    ASSERT(prepared.find(id) != prepared.end());

    Transaction txn = prepared[id];

    ASSERT(txn.id == id);

    for (auto write : txn.writeSet) {
        bool ret = store.put(write.first, // key
                write.second.back(), // value
                timestamp); // timestamp
        ASSERT(ret);
    }

    RetiredTxn rTxn(txn, COMMITTED);
    prepared.erase(id);
    retired.push_back(make_pair(op, rTxn));
}

void
OCCStore::uncommit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    // do some uncommit stuff
    Debug("[%" PRIu64 "] UNDO COMMIT", id);

    Transaction txn = getRetiredTxn(op, id, COMMITTED);

    for (auto write : txn.writeSet) {
        pair<uint64_t, string> val;
	bool ret = store.remove(write.first, val);
	ASSERT(ret);
        ASSERT(val.first == timestamp);
        ASSERT(val.second == write.second.back());
    }

    ASSERT(prepared.find(id) == prepared.end());
    prepared[id] = txn;
}

void
OCCStore::abortTxn(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] ABORT", id);

    if (running.find(id) != running.end()) {
        RetiredTxn txn(running[id], ABORTED_RUNNING);
        retired.push_back(make_pair(op, txn));
        running.erase(id);
    } else if (prepared.find(id) != prepared.end()) {
        RetiredTxn txn(prepared[id], ABORTED_PREPARED);
        retired.push_back(make_pair(op, txn));
        prepared.erase(id);
        //ASSERT(found);
    } else {
        NOT_REACHABLE();
    }
}

void
OCCStore::unabort(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] UNDO ABORT", id);

    pair<opnum_t,RetiredTxn> t = retired.back();
    RetiredTxn rTxn = t.second;

    ASSERT(t.first == op);
    ASSERT(rTxn.txn.id == id);
    ASSERT(rTxn.state == ABORTED_PREPARED || rTxn.state == ABORTED_RUNNING);

    if (rTxn.state == ABORTED_PREPARED) {
        ASSERT(prepared.find(id) == prepared.end());
        prepared[id] = rTxn.txn;
    } else if (rTxn.state == ABORTED_RUNNING) {
        ASSERT(running.find(id) == running.end());
        // revert transaction state back to running 
        running[id] = rTxn.txn;
    }

    retired.pop_back();
}

void
OCCStore::specCommit(opnum_t op)
{
    while ((!retired.empty()) && (retired.front().first <= op)) {
        retired.pop_front();
    }
}

} // namespace nistore

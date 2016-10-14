// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/lockstore.cc:
 *   Key-value store with support for S2PL
 *
 **********************************************************************/
#include "nistore/lockstore.h"
#include "lib/assert.h"
#include "lib/message.h"

namespace nistore {

using namespace std;

bool
LockStore::Transaction::operator==(const Transaction &t)
{
    return id == t.id;
}

LockStore::LockStore() :
   store() { }
LockStore::~LockStore() { }

// Gets the running transaction. Creates one if there isn't one for this client
LockStore::Transaction&
LockStore::getTxn(uint64_t id)
{
    if (running.find(id) == running.end()) {
        Transaction txn(id);
        running[id] = txn;
    } else {
        ASSERT(running[id].id == id);
    }

    return running[id];
}

LockStore::Transaction
LockStore::getRetiredTxn(opnum_t op, uint64_t id, RetiredState state)
{
    pair<opnum_t,RetiredTxn> t = retired.back();
    RetiredTxn rTxn = t.second;

    ASSERT(t.first == op);
    ASSERT(rTxn.txn.id == id);
    ASSERT(rTxn.state == state);

    retired.pop_back();
    return rTxn.txn;
}

/*
 * Used on commit and abort for second phase of 2PL
 */
void
LockStore::dropLocks(const Transaction &txn)
{
    for (auto &write : txn.writeSet) {
	locks.releaseForWrite(write.first, txn.id);
    }

    for (auto &read : txn.readSet) {
	locks.releaseForRead(read.first, txn.id);
    }
}
    
/*
 * This is only used for rollback. Should be able to pick up all locks
 * again bacause we were holding them before.
 */
void
LockStore::getLocks(const Transaction &txn)
{
    bool ret;
    for (auto &write : txn.writeSet) {
	ret = locks.lockForWrite(write.first, txn.id);
	ASSERT(ret);
    }

    for (auto &read : txn.readSet) {
	ret = locks.lockForRead(read.first, txn.id);
	ASSERT(ret);
    }
}

void
LockStore::begin(uint64_t id)
{
    Debug("[%" PRIu64 "] BEGIN", id);
    Transaction txn(id);
    running[id] = txn;
}

void
LockStore::unbegin(uint64_t id)
{
    Debug("[%" PRIu64 "] UNDO BEGIN", id);
    if (running.find(id) != running.end()) {
        running.erase(id);
    }
}

int
LockStore::get(uint64_t id, const string &key, string &value)
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
    } else {
        ret = store.get(key, value);
	
        if (!ret) {
            // couldn't find the key
            return -1;
        }

        // have we read this before?
        if (txn.readSet.find(key) != txn.readSet.end()) {
            txn.readSet[key]++;
        } else {
            // grab the lock
            if (locks.lockForRead(key, id)) {
                txn.readSet[key] = 1;
                return 0;
            } else {
                return -2;
            }
        }
        return 0;
    }
}

void
LockStore::unget(uint64_t id, const string &key)
{
    Debug("[%" PRIu64 "] UNDO GET %s", id, key.c_str());

    if (running.find(id) != running.end()) {
	Transaction &txn = running[id];

	// if we find this in the read set
	if (txn.readSet.find(key) != txn.readSet.end()) {
	    int &read = txn.readSet[key];
	    if (read > 1) {
		read--;
	    } else {
		txn.readSet.erase(key);
		locks.releaseForRead(key, id);
	    }
	}
	// we may not find the read if it was a self-read, etc.
    } else {
	// we should find the transaction
	NOT_REACHABLE();
    }
}

int
LockStore::put(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] PUT %s %s", id, key.c_str(), value.c_str());
    Transaction &txn = getTxn(id);

    if (locks.lockForWrite(key, id)) {
	// record the write
	txn.writeSet[key].push_back(value);
	return 0;
    } else {
	return -2;
    }
}

void
LockStore::unput(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] UNDO PUT %s %s", id, key.c_str(), value.c_str());

    if (running.find(id) != running.end() &&
	running[id].writeSet.find(key) != running[id].writeSet.end()) {
	list<string> &writes = running[id].writeSet[key]; 

	if (writes.back() == value) {
	    writes.pop_back();
	    if (writes.empty()) {
		locks.releaseForWrite(key, id);
	    }
	} else {
	    NOT_REACHABLE();
	}
    } else {
	NOT_REACHABLE();
    }
}

int
LockStore::prepare(uint64_t id, opnum_t op)
{    
    Debug("[%" PRIu64 "] START PREPARE", id);

    if (running.find(id) == running.end()) {
        Warning("Could not find transaction [%" PRIu64 "] to prepare", id);
        return -1;
    } else {
        Transaction txn = running[id];

        running.erase(id);
        prepared[id] = txn;
        Debug("[%" PRIu64 "] PREPARED TO COMMIT", id);
        return 0;
    }
}

void
LockStore::unprepare(uint64_t id, opnum_t op)
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
LockStore::commit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    Debug("[%" PRIu64 "] COMMIT", id);
    ASSERT(prepared.find(id) != prepared.end());

    Transaction txn = prepared[id];

    ASSERT(txn.id == id);


    for (auto write : txn.writeSet) {
        bool ret = store.put(write.first, // key
			     write.second.back()); // value
        ASSERT(ret);
    }

    //drop locks
    dropLocks(txn);

    RetiredTxn rTxn(txn, COMMITTED);
    prepared.erase(id);
    retired.push_back(make_pair(op, rTxn));
}

void
LockStore::uncommit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    // do some uncommit stuff
    Debug("[%" PRIu64 "] UNDO COMMIT", id);

    Transaction txn = getRetiredTxn(op, id, COMMITTED);

    for (auto write : txn.writeSet) {
        string val;
	bool ret = store.remove(write.first, val);
	ASSERT(ret);
        ASSERT(val == write.second.back());
    }

    // pick up all dropped locks
    getLocks(txn);

    ASSERT(prepared.find(id) == prepared.end());
    prepared[id] = txn;
}

void
LockStore::abortTxn(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] ABORT", id);
    Transaction txn;

    if (running.find(id) != running.end()) {
	txn = running[id];
	dropLocks(txn);
        RetiredTxn rTxn(txn, ABORTED_RUNNING);
        retired.push_back(make_pair(op, rTxn));
        running.erase(id);
    } else if (prepared.find(id) != prepared.end()) {
	txn = prepared[id];
	dropLocks(txn);
        RetiredTxn rTxn(txn, ABORTED_PREPARED);
        retired.push_back(make_pair(op, rTxn));
        prepared.erase(id);
    } else {
        NOT_REACHABLE();
    }
}

void
LockStore::unabort(uint64_t id, opnum_t op)
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

    // pick up all of the dropped locks again
    getLocks(rTxn.txn);

    retired.pop_back();
}

void
LockStore::specCommit(opnum_t op)
{
    while ((!retired.empty()) && (retired.front().first <= op)) {
        retired.pop_front();
    }
}

} // namespace nistore

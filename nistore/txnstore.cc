// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/txnstore.cc:
 *   Transactional Key-value store interface
 *
 **********************************************************************/
#include "nistore/txnstore.h"
#include "lib/assert.h"
#include "lib/message.h"

namespace nistore {

using namespace std;

TxnStore::TxnStore() { }
TxnStore::~TxnStore() { }

void
TxnStore::begin(uint64_t id)
{
    Debug("[%" PRIu64 "] BEGIN", id);
}

void
TxnStore::unbegin(uint64_t id)
{
    Debug("[%" PRIu64 "] UNDO BEGIN", id);
}

int
TxnStore::get(uint64_t id, const string &key, string &value)
{
    Debug("[%" PRIu64 "] GET %s", id, key.c_str());
    return 0;
}

void
TxnStore::unget(uint64_t id, const string &key)
{
    Debug("[%" PRIu64 "] UNDO GET %s", id, key.c_str());
}

int
TxnStore::put(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] PUT %s %s", id, key.c_str(), value.c_str());
    return 0;
}

void
TxnStore::unput(uint64_t id, const string &key, const string &value)
{
    Debug("[%" PRIu64 "] UNDO PUT %s %s", id, key.c_str(), value.c_str());
}

int
TxnStore::prepare(uint64_t id, opnum_t op)
{    
    Debug("[%" PRIu64 "] START PREPARE", id);
    return 0;
}

void
TxnStore::unprepare(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] UNDO PREPARE", id);
}

void
TxnStore::commit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    Debug("[%" PRIu64 "] COMMIT", id);
}

void
TxnStore::uncommit(uint64_t id, uint64_t timestamp, opnum_t op)
{
    // do some uncommit stuff
    Debug("[%" PRIu64 "] UNDO COMMIT", id);

}

void
TxnStore::abortTxn(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] ABORT", id);
}

void
TxnStore::unabort(uint64_t id, opnum_t op)
{
    Debug("[%" PRIu64 "] UNDO ABORT", id);
}

void
TxnStore::specCommit(opnum_t op)
{
    Debug("SPEC COMMIT");
}

} // namespace nistore

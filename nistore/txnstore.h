// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/txnstore.h:
 *   Interface for a single node transactional store serving as a
 *   server-side backend for NiStore 
 *
 **********************************************************************/

#ifndef _TXN_STORE_H_
#define _TXN_STORE_H_

#include "lib/viewstamp.h"
#include <string>

namespace nistore {

using namespace std;

class TxnStore
{
public:

    TxnStore();
    ~TxnStore();

    // begin a transaction
    virtual void begin(uint64_t id);
    // add key to read set
    virtual int get(uint64_t id, const string &key, string &value);
    // add key to write set
    virtual int put(uint64_t id, const string &key, const string &value);
    // check whether we can commit or abort this transaction
    // and lock the read/write set
    virtual int prepare(uint64_t id, opnum_t op);
    // commit the transaction
    virtual void commit(uint64_t id, uint64_t timestamp, opnum_t op);
    // abort a running transaction
    virtual void abortTxn(uint64_t id, opnum_t op);

    // undo operations from Spec Paxos
    virtual void unbegin(uint64_t id);
    virtual void unget(uint64_t id, const string &key);
    virtual void unput(uint64_t id, const string &key, const string &value);
    virtual void unprepare(uint64_t id, opnum_t op);
    virtual void uncommit(uint64_t id, uint64_t timestamp, opnum_t op);
    virtual void unabort(uint64_t id, opnum_t op);

    // upcall from Spec Paxos to clean up
    virtual void specCommit(opnum_t op);
};

} // namespace nistore

#endif /* _TXN_STORE_H_ */

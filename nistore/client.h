// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/client.h:
 *   NiStore client-side logic and APIs
 *
 **********************************************************************/

#ifndef _NI_CLIENT_H_
#define _NI_CLIENT_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "common/client.h"
#include "lib/configuration.h"
#include "spec/client.h"
#include "vr/client.h"
#include "fastpaxos/client.h"
#include "nistore/request.pb.h"

#include <iostream>
#include <cstdlib>
#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <set>

namespace nistore {

using namespace std;

enum Proto {
    PROTO_UNKNOWN,
    PROTO_VR,
    PROTO_SPEC,
    PROTO_FAST
};

class Client
{
public:
    /* Constructor needs path to shard configs and number of shards. */
    Client(Proto mode, string configPath, int nshards);
    ~Client();

    /* API Calls for NiStore. */
    void Begin();
    bool Get(const string &key, string &value);
    void Put(const string &key, const string &value);
    bool Commit();
    void Abort();

private:
    long client_id; // Unique ID for this client.
    long nshards; // Number of shards in niStore

    UDPTransport transport; // Transport used by paxos client proxies.
    thread *clientTransport; // Thread running the transport event loop.

    vector<specpaxos::Client *> shard; // List of shard client proxies.
    specpaxos::Client *tss; // Timestamp server shard.

    mutex cv_m; // Synchronize access to all state in this class and cv.
    condition_variable cv; // To block api calls till a replica reply.

    /* Transaction specific variables. */
    bool op_pending; // True if a transaction is ongoing.
    bool status; // Whether to commit transaction & reply status.
    unsigned int nreplies; // Number of replies received back in 2PC.
    set<int> all_participants; // Participants in ongoing transaction.
    set<int> yes_participants; // Participants who replies YES.
    string replica_reply; // Reply back from a shard.

    /* Private helper functions. */
    void run_client(); // Runs the transport event loop.
    void send_begin(unsigned int); // Sends BEGIN message to shard[i].

    /* Callbacks for hearing back from a shard for an operation. */
    void beginCallback(const int, const string &, const string &);
    void getCallback(const int, const string &, const string &);
    void putCallback(const int, const string &, const string &);
    void prepareCallback(const int, const string &, const string &);
    void commitCallback(const int, const string &, const string &);
    void abortCallback(const int, const string &, const string &);

    void tssCallback(const string &request, const string &reply);

    // Sharding logic: Given key, generates a number b/w 0 to nshards-1
    long key_to_shard(const string &key);
};

} // namespace nistore

#endif /* _NI_CLIENT_H_ */

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/client.cc:
 *   Single NiStore client. Implements the API functionalities.
 *
 **********************************************************************/

#include "nistore/client.h"

namespace nistore {

Client::Client(Proto mode, string configPath, int nShards)
    : transport(0.0, 0.0, 0)
{
    // Initialize all state here;
    struct timeval t1;
    gettimeofday(&t1, NULL);
    srand(t1.tv_sec + t1.tv_usec);
    client_id = rand();

    nshards = nShards;
    shard.reserve(nshards);
    op_pending = false;

    Debug("Initializing NiStore client with id [%lu]", client_id);

    /* Start a client for time stamp server. */
    string tssConfigPath = configPath + ".tss.config";
    ifstream tssConfigStream(tssConfigPath);
    if (tssConfigStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                tssConfigPath.c_str());
        exit(0);
    }
    specpaxos::Configuration tssConfig(tssConfigStream);
    tss = new specpaxos::vr::VRClient(tssConfig, &transport);
    /*
    switch (mode) {
        case PROTO_VR:
            tss = new specpaxos::vr::VRClient(tssConfig, &transport);
            break;
        case PROTO_SPEC:
            tss = new specpaxos::spec::SpecClient(tssConfig, &transport);
            break;
        case PROTO_FAST:
            tss = new specpaxos::fastpaxos::FastPaxosClient(tssConfig, &transport);
            break;
        default:
            NOT_REACHABLE();
    }*/

    /* Start a client for each shard. */
    for (int i = 0; i < nShards; i++) {
        string shardConfigPath = configPath + to_string(i) + ".config";
        ifstream shardConfigStream(shardConfigPath);
        if (shardConfigStream.fail()) {
            fprintf(stderr, "unable to read configuration file: %s\n",
                    shardConfigPath.c_str());
            exit(0);
        }
        specpaxos::Configuration shardConfig(shardConfigStream);

        switch (mode) {
            case PROTO_VR:
                shard[i] = new specpaxos::vr::VRClient(shardConfig, &transport);
                break;
            case PROTO_SPEC:
                shard[i] = new specpaxos::spec::SpecClient(shardConfig, &transport);
                break;
            case PROTO_FAST:
                shard[i] = new specpaxos::fastpaxos::FastPaxosClient(shardConfig, &transport);
                break;
            default:
                NOT_REACHABLE();
        }
    }

    /* Run the transport in a new thread. */
    clientTransport = new thread(&Client::run_client, this);

    Debug("NiStore client [%lu] created!", client_id);
}

Client::~Client()
{
    // TODO: Consider killing transport and associated thread.
}

/* Runs the transport event loop. */
void
Client::run_client()
{
    transport.Run();
}

/* Sends BEGIN to a single shard indexed by i. */
void
Client::send_begin(unsigned int i)
{
    Debug("[shard %d] Sending BEGIN", i);
    string request_str;
    Request request;
    request.set_op(Request::BEGIN);
    request.set_txnid(client_id);
    request.SerializeToString(&request_str);

    transport.Timer(0, [=]() {
        shard[i]->Invoke(request_str, 
                         bind(&Client::beginCallback,
                              this, i,
                              placeholders::_1,
                              placeholders::_2));
    });
}
  

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void
Client::Begin()
{
    // Initialize all transaction variables.
    Debug("BEGIN Transaction");
    op_pending = true;
    all_participants.clear();
    yes_participants.clear();

    // Nothing else to do, return.
}

/* Returns the value corresponding to the supplied key. */
bool
Client::Get(const string &key, string &value)
{
    // Contact the appropriate shard to get the value.
    unique_lock<mutex> lk(cv_m);

    int i = key_to_shard(key);

    // If needed, add this shard to set of participants and send BEGIN.
    if (all_participants.find(i) == all_participants.end()) {
        all_participants.insert(i);
        send_begin(i);

        // Wait for reply from the shard.
        Debug("[shard %d] Waiting for BEGIN reply", i);
        cv.wait(lk);
        Debug("[shard %d] BEGIN reply received", i);
    }

    // Send the GET operation to appropriate shard.
    Debug("[shard %d] Sending GET [%s]", i, key.c_str());
    string request_str;
    Request request;
    request.set_op(Request::GET);
    request.set_txnid(client_id);
    request.set_arg0(key);
    request.SerializeToString(&request_str);

    transport.Timer(0, [=]() {
        shard[i]->Invoke(request_str,
                          bind(&Client::getCallback,
                          this, i,
                          placeholders::_1,
                          placeholders::_2));
    });

    // Wait for reply from shard.
    Debug("[shard %d] Waiting for GET reply", i);
    cv.wait(lk);
    Debug("[shard %d] GET reply received", i);

    // Reply from shard should be available in "replica_reply".
    value = replica_reply;
    return status;
}

/* Sets the value corresponding to the supplied key. */
void
Client::Put(const string &key, const string &value)
{
    // Contact the appropriate shard to set the value.
    unique_lock<mutex> lk(cv_m);

    int i = key_to_shard(key);

    // If needed, add this shard to set of participants and send BEGIN.
    if (all_participants.find(i) == all_participants.end()) {
        all_participants.insert(i);
        send_begin(i);

        // Wait for reply from the shard.
        Debug("[shard %d] Waiting for BEGIN reply", i);
        cv.wait(lk);
        Debug("[shard %d] BEGIN reply received", i);
    }

    Debug("[shard %d] Sending PUT [%s]", i, key.c_str());
    string request_str;
    Request request;
    request.set_op(Request::PUT);
    request.set_txnid(client_id);
    request.set_arg0(key);
    request.set_arg1(value);
    request.SerializeToString(&request_str);

    transport.Timer(0, [=]() {
        shard[i]->Invoke(request_str,
                          bind(&Client::putCallback,
                          this, i,
                          placeholders::_1,
                          placeholders::_2));
    });

    // Wait for reply from shard.
    Debug("[shard %d] Waiting for PUT reply", i);
    cv.wait(lk);
    Debug("[shard %d] PUT reply received", i);

    // PUT operation should have suceeded. Return.
}

/* Attempts to commit the ongoing transaction. */
bool
Client::Commit()
{
    // Implementing 2 Phase Commit

    // 1. Send commit-prepare to all shards.
    {
        unique_lock<mutex> lk(cv_m);

        Debug("PREPARE Transaction");
        string request_str;
        Request request;
        request.set_op(Request::PREPARE);
        request.set_txnid(client_id);
        request.SerializeToString(&request_str);
        
        nreplies = 0;
        status = true;

        for (set<int>::iterator it = all_participants.begin(); 
              it != all_participants.end(); it++) {
            Debug("[shard %d] Sending prepare", *it);
            transport.Timer(0, [=]() {
                shard[*it]->Invoke(request_str,
                                    bind(&Client::prepareCallback,
                                    this, *it,
                                    placeholders::_1,
                                    placeholders::_2));
            });
        }

        // 2. Wait for reply from all shards. (abort on timeout)
        Debug("Waiting for PREPARE replies");
        cv.wait(lk);
        Debug("All PREPARE replies received");
    }

    // 3. If all votes YES, send commit to all shards.
    if (nreplies == all_participants.size() && status == true) {
        // Get a timestamp from the tss
        {
            unique_lock<mutex> lk(cv_m);

            Debug("Sending request to TSS");
            transport.Timer(0, [=]() {
                tss->Invoke("", bind(&Client::tssCallback, this,
                                      placeholders::_1,
                                      placeholders::_2));
            });

            Debug("Waiting for TSS reply");
            cv.wait(lk);
            Debug("TSS reply received");
        }
        long ts = stol(replica_reply, NULL, 10);

        // Send Commit to all shards
        {
            unique_lock<mutex> lk(cv_m);

            Debug("COMMIT Transaction");
            string request_str;
            Request request;
            request.set_op(Request::COMMIT);
            request.set_txnid(client_id);
            request.set_arg0(to_string(ts));
            request.SerializeToString(&request_str);

            nreplies = 0;
            for (set<int>::iterator it = all_participants.begin(); 
                  it != all_participants.end(); it++) {
                Debug("[shard %d] Sending commit", *it);
                transport.Timer(0, [=]() {
                    shard[*it]->Invoke(request_str,
                                        bind(&Client::commitCallback,
                                        this, *it,
                                        placeholders::_1,
                                        placeholders::_2));
                });
            }

            // Wait for commit to succeed.
            Debug("Waiting for COMMIT replies");
            cv.wait(lk);
            Debug("All COMMIT replies received");
        }
        return true;
    }

    // 4. If not, send abort to all shards.
    Abort();
    return false;
}

/* Aborts the ongoing transaction. */
void
Client::Abort()
{
    // Send abort to all shards.
    unique_lock<mutex> lk(cv_m);

    Debug("ABORT Transaction");
    string request_str;
    Request request;
    request.set_op(Request::ABORT);
    request.set_txnid(client_id);
    request.SerializeToString(&request_str);

    nreplies = 0;
    int aborts_sent = 0;
    for (set<int>::iterator it = all_participants.begin(); 
          it != all_participants.end(); it++) {
        if (yes_participants.find(*it) != yes_participants.end()) {
            aborts_sent++;
            Debug("[shard %d] Sending abort", *it);
            transport.Timer(0, [=]() {
                shard[*it]->Invoke(request_str,
                                    bind(&Client::abortCallback,
                                    this, *it,
                                    placeholders::_1,
                                    placeholders::_2));
            });
        }
    }

    if (aborts_sent == 0)
        return;

    // Wait for reply from shard.
    Debug("Waiting for ABORT replies");
    cv.wait(lk);
    Debug("All ABORT replies received");
}

/* Callback from a shard replica on begin operation completion. */
void
Client::beginCallback(const int index, const string &request_str, const string &reply_str)
{
    // BEGIN always returns success, so no need to check reply.
    lock_guard<mutex> lock(cv_m);
    Debug("[shard %d] BEGIN callback", index);

    cv.notify_all();
}

/* Callback from a shard replica on get operation completion. */
void
Client::getCallback(const int index, const string &request_str, const string &reply_str)
{
    lock_guard<mutex> lock(cv_m);

    // Copy reply to "replica_reply".
    Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %d] GET callback [%d]", index, reply.status());

    if (reply.status() >= 0) {
        status = true;
        replica_reply = reply.value();
    } else {
        status = false;
    }
    
    // Wake up thread waiting for the reply.
    cv.notify_all();
}

/* Callback from a shard replica on put operation completion. */
void
Client::putCallback(const int index, const string &request_str, const string &reply_str)
{
    lock_guard<mutex> lock(cv_m);

    // PUTs always returns success, so no need to check reply.
    Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %d] PUT callback [%d]", index, reply.status());

    // Wake up thread waiting for the reply.
    cv.notify_all();
}

/* Callback from a shard replica on prepare operation completion. */
void
Client::prepareCallback(const int index, const string &request_str, const string &reply_str)
{
    lock_guard<mutex> lock(cv_m);
    Debug("[shard %d] PREPARE callback str: %s", index, reply_str.c_str());

    Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %d] PREPARE callback [%d]", index, reply.status());

    // If "NO" vote, set commit decision = false, and wake up the
    // prepare call waiting for replies.
    if (reply.status() < 0) {
        status = false;
    } else {
        yes_participants.insert(index);
    }

    // Increment number of replies. If nreplies == nshards, all replies
    // have been received, so wake up the prepare call.
    nreplies++;
    if (nreplies == all_participants.size()) {
        cv.notify_all();
    }
}

/* Callback from a shard replica on commit operation completion. */
void
Client::commitCallback(const int index, const string &request_str, const string &reply_str)
{
    lock_guard<mutex> lock(cv_m);

    // COMMITs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %d] COMMIT callback [%d]", index, reply.status());

    nreplies++;
    if (nreplies == all_participants.size()) {
        cv.notify_all();
    }
}

/* Callback from a shard replica on abort operation completion. */
void
Client::abortCallback(const int index, const string &request_str, const string &reply_str)
{
    lock_guard<mutex> lock(cv_m);

    // ABORTs always succeed.
    Reply reply;
    reply.ParseFromString(reply_str);
    Debug("[shard %d] ABORT callback [%d]", index, reply.status());

    nreplies++;
    if (nreplies == yes_participants.size()) {
        cv.notify_all();
    }
}

/* Callback from a tss replica upon any request. */
void
Client::tssCallback(const string &request, const string &reply)
{
    lock_guard<mutex> lock(cv_m);
    Debug("TSS callback [%s]", reply.c_str());

    // Copy reply to "replica_reply".
    replica_reply = reply;
    
    // Wake up thread waiting for the reply.
    cv.notify_all();
}

/* Takes a key and returns which shard the key is stored in. */
long
Client::key_to_shard(const string &key)
{
    unsigned long hash = 0;
    const char* str = key.c_str();
    for (unsigned int i = 0; i < key.length(); i++) {
        hash = hash << 1 ^ str[i];
    }

    return (hash % nshards);
}

} // namespace nistore

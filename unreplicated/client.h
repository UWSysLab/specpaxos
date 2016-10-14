// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated/client.h:
 *   dummy implementation of replication interface that just uses a
 *   single replica and passes commands directly to it
 *
 * Copyright 2013-2016 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#ifndef _UNREPLICATED_CLIENT_H_
#define _UNREPLICATED_CLIENT_H_

#include "common/client.h"
#include "lib/configuration.h"
#include "unreplicated/unreplicated-proto.pb.h"

namespace specpaxos {
namespace unreplicated {
    
class UnreplicatedClient : public Client
{
public:
    UnreplicatedClient(const Configuration &config,
                       Transport *transport,
                       uint64_t clientid = 0);
    virtual ~UnreplicatedClient();
    virtual void Invoke(const string &request, continuation_t continuation);
    virtual void InvokeUnlogged(int replicaIdx,
                                const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT);
    virtual void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data);

protected:
    struct PendingRequest
    {
        string request;
        continuation_t continuation;
        inline PendingRequest(string request, continuation_t continuation)
            : request(request), continuation(continuation) { }
    };
    PendingRequest *pendingRequest;
    PendingRequest *pendingUnloggedRequest;

    void HandleReply(const TransportAddress &remote,
                     const proto::ReplyMessage &msg);
    void HandleUnloggedReply(const TransportAddress &remote,
                             const proto::UnloggedReplyMessage &msg);
};

} // namespace specpaxos::unreplicated
} // namespace specpaxos

#endif  /* _UNREPLICATED_CLIENT_H_ */

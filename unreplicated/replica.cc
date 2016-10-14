// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated.cc:
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

#include "common/replica.h"
#include "unreplicated/replica.h"
#include "unreplicated/unreplicated-proto.pb.h"

#include "lib/message.h"
#include "lib/transport.h"

namespace specpaxos {
namespace unreplicated {

using namespace proto;
    
void
UnreplicatedReplica::HandleRequest(const TransportAddress &remote,
                                   const proto::RequestMessage &msg)
{
    proto::ReplyMessage reply;

    Debug("Received request %s", (char *)msg.req().op().c_str());

    Execute(0, msg.req(), reply);

    // The protocol defines these as required, even if they're not
    // meaningful.
    reply.set_view(0);
    reply.set_opnum(0);

    if (!(transport->SendMessage(this, remote, reply)))
        Warning("Failed to send reply message");
}

void
UnreplicatedReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                           const proto::UnloggedRequestMessage &msg)
{
    proto::UnloggedReplyMessage reply;

    Debug("Received unlogged request %s", (char *)msg.req().op().c_str());

    ExecuteUnlogged(msg.req(), reply);
    
    if (!(transport->SendMessage(this, remote, reply)))
        Warning("Failed to send reply message");
}

UnreplicatedReplica::UnreplicatedReplica(Configuration config,
                                         int myIdx,
                                         bool initialize,
                                         Transport *transport,
                                         AppReplica *app)
    : Replica(config, myIdx, initialize, transport, app)
{
    if (!initialize) {
        Panic("Recovery does not make sense for unreplicated mode");
    }

    this->status = STATUS_NORMAL;
}

void
UnreplicatedReplica::ReceiveMessage(const TransportAddress &remote,
                                    const string &type, const string &data)
{
    static proto::RequestMessage request;
    static proto::UnloggedRequestMessage unloggedRequest;
    
    if (type == request.GetTypeName()) {
        request.ParseFromString(data);
        HandleRequest(remote, request);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnloggedRequest(remote, unloggedRequest);
    } else {
        Panic("Received unexpected message type in unreplicated proto: %s",
              type.c_str());
    }
}

} // namespace specpaxos::unreplicated
} // namespace specpaxos

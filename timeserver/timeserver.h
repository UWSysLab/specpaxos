// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * timeserver/timeserver.h:
 *   Timeserver API
 *
 **********************************************************************/

#ifndef _TIME_SERVER_H_
#define _TIME_SERVER_H_

#include "lib/configuration.h"
#include "common/replica.h"
#include "lib/udptransport.h"
#include "spec/replica.h"
#include "vr/replica.h"
#include "fastpaxos/replica.h"

#include <string>

using namespace std;

class TimeStampServer : public specpaxos::AppReplica
{
public:
    TimeStampServer();
    ~TimeStampServer();

    void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2);
    void RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap);
    void CommitUpcall(opnum_t op);
private:
    long ts;
    string newTimeStamp();

};
#endif /* _TIME_SERVER_H_ */

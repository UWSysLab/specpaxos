// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/lockserver.h:
 *   Simple multi-reader, single-writer lock server
 *
 **********************************************************************/

#ifndef _LOCK_SERVER_H_
#define _LOCK_SERVER_H_

#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iostream>
#include <unordered_set>
#include <queue>
#include <sys/time.h>
#include <map>

namespace nistore {
using namespace std;

#define LOCK_WAIT_TIMEOUT 5000

class LockServer
{

public:
    LockServer();
    ~LockServer();

    bool lockForRead(const string &lock, uint64_t requester);
    bool lockForWrite(const string &lock, uint64_t requester);
    void releaseForRead(const string &lock, uint64_t holder);
    void releaseForWrite(const string &lock, uint64_t holder);

private:
    enum LockState {
	UNLOCKED,
	LOCKED_FOR_READ,
	LOCKED_FOR_WRITE,
	LOCKED_FOR_READ_WRITE
    };

    struct Waiter {
        bool write;
        struct timeval waitTime;

        Waiter() {write = false;}
        Waiter(bool w) {
            gettimeofday(&waitTime, NULL);
            write = w;
        }

        bool checkTimeout(const struct timeval &now);
    };

    struct Lock {
        LockState state;
        unordered_set<uint64_t> holders;
        queue<uint64_t> waitQ;
        map<uint64_t, Waiter> waiters;

        Lock() {
            state = UNLOCKED;
        };
        void waitForLock(uint64_t requester, bool write);
        bool tryAcquireLock(uint64_t requester, bool write);
        bool isWriteNext();
    };
	

    /* Global store which keep key -> (timestamp, value) list. */
    unordered_map<string, Lock> locks;

    uint64_t readers;
    uint64_t writers;
};

} // namespace nistore

#endif  /* _LOCK_SERVER_H_ */

// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/server.cc:
 *   Single NiStore server
 *
 **********************************************************************/
#include "nistore/server.h"
#include "nistore/request.pb.h"

namespace nistore {

using namespace specpaxos;

void
Server::ReplicaUpcall(opnum_t opnum, const string &str1, string &str2)
{
    Request request;
    Reply reply;
    int status;

    request.ParseFromString(str1);

    switch (request.op()) {

    case Request::BEGIN:
        store.begin(request.txnid());
        reply.set_status(0);
        break;

    case Request::GET:
    {
        string val;
        status = store.get(request.txnid(), request.arg0(), val);
        reply.set_value(val);            
        break;
    }

    case Request::PUT:
        status = store.put(request.txnid(), request.arg0(), request.arg1());
        break;

    case Request::PREPARE:
        status = store.prepare(request.txnid(), opnum);
        break;

    case Request::COMMIT:
    {
        long timestamp = stol(request.arg0());
        store.commit(request.txnid(), timestamp, opnum);
        status = 0;
        break;
    }

    case Request::ABORT:
        store.abortTxn(request.txnid(), opnum);
        status = 0;
	break;
	
    default:
        Panic("Unrecognized operation.");
    }
    reply.set_status(status);
    reply.SerializeToString(&str2);
}

void
Server::RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap)
{
    for (auto rit = opMap.rbegin(); rit != opMap.rend(); rit++) {
        Request request;
        opnum_t opnum = rit->first;

        request.ParseFromString(rit->second);

        switch (request.op()) {

        case Request::BEGIN:
            store.unbegin(request.txnid());
            break;

        case Request::GET:
            store.unget(request.txnid(), request.arg0());
            break;

        case Request::PUT:
            store.unput(request.txnid(), request.arg0(), request.arg1());
            break;

        case Request::PREPARE:
            store.unprepare(request.txnid(), opnum);
            break;

        case Request::COMMIT:
        {
            long timestamp = stol(request.arg0());
            store.uncommit(request.txnid(), timestamp, opnum);
            break;
        }

        case Request::ABORT:
            store.unabort(request.txnid(), opnum);
            break;

        default:
            Panic("Unrecognized operation.");
        }
    }
}

void
Server::CommitUpcall(opnum_t opnum)
{
    store.specCommit(opnum);
}

}

static void Usage(const char *progName)
{
    fprintf(stderr, "usage: %s -c conf-file -i replica-index\n",
            progName);
    exit(1);
}

int
main(int argc, char **argv)
{
    int index = -1;
    const char *configPath = NULL;
    enum {
        PROTO_UNKNOWN,
        PROTO_VR_LOCKING,
        PROTO_SPEC_LOCKING,
        PROTO_VR_OCC,
        PROTO_SPEC_OCC,
        PROTO_FAST_OCC,
    } proto = PROTO_UNKNOWN;

  // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:i:m:")) != -1) {
        switch (opt) {
            case 'c':
                configPath = optarg;
                break;

            case 'i':
            {
                char *strtolPtr;
                index = strtoul(optarg, &strtolPtr, 10);
                if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0))
                {
                    fprintf(stderr,
                            "option -i requires a numeric arg\n");
                    Usage(argv[0]);
                }
                break;
            }

            case 'm':
            {
                if (strcasecmp(optarg, "vr-l") == 0) {
                    proto = PROTO_VR_LOCKING;
                } else if (strcasecmp(optarg, "spec-l") == 0) {
                    proto = PROTO_SPEC_LOCKING;
                } else if (strcasecmp(optarg, "vr-occ") == 0) {
                    proto = PROTO_VR_OCC;
                } else if (strcasecmp(optarg, "spec-occ") == 0) {
                    proto = PROTO_SPEC_OCC;
                } else if (strcasecmp(optarg, "fast-occ") == 0) {
                    proto = PROTO_FAST_OCC;
                } else {
                    fprintf(stderr, "unknown mode '%s'\n", optarg);
                    Usage(argv[0]);
                }
                break;
            }

            default:
                fprintf(stderr, "Unknown argument %s\n", argv[optind]);
                break;
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        Usage(argv[0]);
    }

    if (index == -1) {
        fprintf(stderr, "option -i is required\n");
        Usage(argv[0]);
    }

    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    if (index >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", index, config.n);
        Usage(argv[0]);
    }

    UDPTransport transport(0.0, 0.0, 0);

    specpaxos::Replica *replica;
    nistore::Server server;
    switch (proto) {
        case PROTO_VR_LOCKING:
        case PROTO_VR_OCC:
            if (PROTO_VR_LOCKING) {
                server = nistore::Server(true);
            } else {
                server = nistore::Server(false);
            }

            replica = new specpaxos::vr::VRReplica(config, index, true,
                                                   &transport, 1, &server);
            break;

        case PROTO_SPEC_LOCKING:
        case PROTO_SPEC_OCC:
            if (PROTO_SPEC_LOCKING) {
                server = nistore::Server(true);
            } else {
                server = nistore::Server(false);
            }

            replica = new specpaxos::spec::SpecReplica(config, index, true, &transport, &server);
            break;

        case PROTO_FAST_OCC:
            server = nistore::Server(false);

            replica = new specpaxos::fastpaxos::FastPaxosReplica(config, index, true,
                                                                 &transport, &server);

            break;

        default:
            NOT_REACHABLE();
    }
    
    (void)replica;              // silence warning
    transport.Run();

    return 0;
}


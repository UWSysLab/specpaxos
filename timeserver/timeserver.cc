// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * timeserver/timeserver.cc:
 *   Single TimeStamp Server.
 *
 **********************************************************************/

#include "timeserver/timeserver.h"

TimeStampServer::TimeStampServer()
{
    ts = 0;
}

TimeStampServer::~TimeStampServer() { }

string
TimeStampServer::newTimeStamp()
{
    ts++;
    return to_string(ts);
}

void
TimeStampServer::ReplicaUpcall(opnum_t opnum,
                               const string &str1,
                               string &str2)
{
    Debug("Received Upcall: " FMT_OPNUM ", %s", opnum, str1.c_str());
    // Get a new timestamp from the TimeStampServer
    str2 = newTimeStamp();
}

/* Ignore for now, will be used when running specpaxos. */
void
TimeStampServer::RollbackUpcall(opnum_t current,
                                opnum_t to,
                                const std::map<opnum_t, string> &opMap)
{
    Debug("Received Rollback Upcall: " FMT_OPNUM ", " FMT_OPNUM, current, to);
    for (auto op : opMap) {
        ts--;
    }
}


/* Ignore for now, will be used when running specpaxos. */
void
TimeStampServer::CommitUpcall(opnum_t commitOpnum)
{
    Debug("Received Commit Upcall: " FMT_OPNUM, commitOpnum);
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
    PROTO_VR,
    PROTO_SPEC,
    PROTO_FAST
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
          if (strcasecmp(optarg, "vr") == 0) {
            proto = PROTO_VR;
          } else if (strcasecmp(optarg, "spec") == 0) {
            proto = PROTO_SPEC;
          } else if (strcasecmp(optarg, "fast") == 0) {
            proto = PROTO_FAST;
          } else {
            proto = PROTO_VR;
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
    fprintf(stderr, "option -i is required\n");
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
  TimeStampServer server;

  switch (proto) {
      case PROTO_VR:
          replica = new specpaxos::vr::VRReplica(config, index, true, &transport, 1, &server);
          break;

      case PROTO_SPEC:
          replica = new specpaxos::spec::SpecReplica(
              config, index, true, &transport, &server);
          break;

      case PROTO_FAST:
          replica = new specpaxos::fastpaxos::FastPaxosReplica(config, index, true, &transport, &server);
          break;

      default:
        NOT_REACHABLE();
  }

  (void)replica;              // silence warning
  transport.Run();

  return 0;
}

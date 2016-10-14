// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
// vim: set ts=4 sw=4:
/***********************************************************************
 *
 * nistore/benchClient.cc:
 *   Benchmarking client for NiStore.
 *
 **********************************************************************/

#include "nistore/client.h"

using namespace std;

int
main(int argc, char **argv)
{
    const char *configPath = NULL;
    const char *keysPath = NULL;
    int duration = 10;
    int nShards = 1;
    int tLen = 10;
    int wPer = 50; // Out of 100
    int skew = 0; // difference between real clock and TrueTime
    int error = 0; // error bars

    vector<string> keys;
    string key, value;
    int nKeys = 100;

    nistore::Proto mode = nistore::PROTO_UNKNOWN;

    int opt;
    while ((opt = getopt(argc, argv, "c:d:N:l:w:k:f:m:e:s:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        { 
            configPath = optarg;
            break;
        }

        case 'f': // Generated keys path
        { 
            keysPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        { 
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'd': // Duration in seconds to run.
        { 
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (duration <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'l': // Length of each transaction (deterministic!)
        {
            char *strtolPtr;
            tLen = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (tLen <= 0)) {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;
        }

        case 'w': // Percentage of writes (out of 100)
        {
            char *strtolPtr;
            wPer = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (wPer < 0 || wPer > 100)) {
                fprintf(stderr, "option -w requires a arg b/w 0-100\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }
        case 's':
        {
            char *strtolPtr;
            skew = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (skew < 0))
            {
                fprintf(stderr,
                        "option -s requires a numeric arg\n");
            }
            break;
        }
        case 'e':
        {
            char *strtolPtr;
            error = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (error < 0))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'm': // Mode to run in [spec/vr/...]
        {
            if (strcasecmp(optarg, "spec-l") == 0) {
                mode = nistore::PROTO_SPEC;
            } else if (strcasecmp(optarg, "spec-occ") == 0) {
                mode = nistore::PROTO_SPEC;
            } else if (strcasecmp(optarg, "vr-l") == 0) {
                mode = nistore::PROTO_VR;
            } else if (strcasecmp(optarg, "vr-occ") == 0) {
                mode = nistore::PROTO_VR;
            } else if (strcasecmp(optarg, "fast-occ") == 0) {
                mode = nistore::PROTO_FAST;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
        }
    }

    if (mode == nistore::PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        exit(0);
    }

    nistore::Client client(mode, configPath, nShards);

    // Read in the keys from a file and populate the key-value store.
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    
    struct timeval t0, t1, t2, t3, t4;

    int nTransactions = 0; // Number of transactions attempted.
    int tCount = 0; // Number of transaction suceeded.
    double tLatency = 0.0; // Total latency across all transactions.
    int getCount = 0;
    double getLatency = 0.0;
    int putCount = 0;
    double putLatency = 0.0;
    int beginCount = 0;
    double beginLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    while (1) {
        gettimeofday(&t1, NULL);
        client.Begin();
        gettimeofday(&t4, NULL);
        
        beginCount++;
        beginLatency += ((t4.tv_sec - t1.tv_sec)*1000000 + (t4.tv_usec - t1.tv_usec));

        for (int j = 0; j < tLen; j++) {
            // Uniform selection of keys.
            key = keys[rand() % nKeys];

            // Zipf-like selection of keys.
            /*
            int r = log(RAND_MAX/(1.0+rand()))*nKeys/100;
            if (r > nKeys) r = r % nKeys;
            key = keys[r];
            */

            if (rand() % 100 < wPer) {
                //value = random_string();
                gettimeofday(&t3, NULL);
                client.Put(key, key);
                gettimeofday(&t4, NULL);
                
                putCount++;
                putLatency += ((t4.tv_sec - t3.tv_sec)*1000000 + (t4.tv_usec - t3.tv_usec));
            } else {
                gettimeofday(&t3, NULL);
                client.Get(key, value);
                gettimeofday(&t4, NULL);

                getCount++;
                getLatency += ((t4.tv_sec - t3.tv_sec)*1000000 + (t4.tv_usec - t3.tv_usec));
            }
        }

        gettimeofday(&t3, NULL);
        bool status = client.Commit();
        gettimeofday(&t2, NULL);

        commitCount++;
        commitLatency += ((t2.tv_sec - t3.tv_sec)*1000000 + (t2.tv_usec - t3.tv_usec));

        long latency = (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);

        fprintf(stderr, "%d %ld.%06ld %ld.%06ld %ld %d\n", nTransactions+1, t1.tv_sec,
                (long)t1.tv_usec, t2.tv_sec, (long)t2.tv_usec, latency, status?1:0);

        if (status) {
            tCount++;
            tLatency += latency;
        }
        nTransactions++;

        gettimeofday(&t1, NULL);
        if ( ((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > duration*1000000) 
            break;
    }

    printf("# Commit_Ratio: %lf\n", (double)tCount/nTransactions);
    printf("# Overall_Latency: %lf\n", tLatency/tCount);
    printf("# Begin: %d, %lf\n", beginCount, beginLatency/beginCount);
    printf("# Get: %d, %lf\n", getCount, getLatency/getCount);
    printf("# Put: %d, %lf\n", putCount, putLatency/putCount);
    printf("# Commit: %d, %lf\n", commitCount, commitLatency/commitCount);
    
    exit(0);
    return 0;
}

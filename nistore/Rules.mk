d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc txnstore.cc server.cc \
	lockstore.cc lockserver.cc kvstore.cc \
	benchClient.cc versionedKVStore.cc occstore.cc)

PROTOS += $(addprefix $(d), \
	    request.proto)

LIB-kvstore := $(o)kvstore.o

LIB-stores := $(o)lockserver.o $(o)versionedKVStore.o

OBJS-ni-store := $(o)server.o $(o)txnstore.o $(o)lockstore.o $(o)occstore.o

OBJS-ni-client := $(o)request.o $(o)client.o \
  $(OBJS-spec-client) $(OBJS-vr-client) $(OBJS-fastpaxos-client) $(LIB-udptransport)

$(d)benchClient: $(OBJS-ni-client) $(o)benchClient.o

$(d)replica: $(o)request.o $(OBJS-ni-store) $(LIB-kvstore) $(LIB-stores) \
	$(OBJS-spec-replica) $(OBJS-vr-replica) $(OBJS-fastpaxos-replica) $(LIB-udptransport)

BINS += $(d)benchClient $(d)replica

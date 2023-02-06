package kvraft

import (
    "6.824/labrpc"
    "math/big"
    "crypto/rand"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
    leaderId   int
    id         int64
    sequenceId int64
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
    ck.leaderId = 0
    ck.id = nrand()
    ck.sequenceId = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    ck.sequenceId++

    args := GetArgs{
        Key : key,
        ClientId : ck.id,
        SequenceId : ck.sequenceId,
    }

    //DPrintf("Get key [%v]\n", args.Key)

    for {
        reply := GetReply{}
        ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

        if ok {
            if reply.Err == OK {
                //DPrintf("Get key:[%v], value:[%v]\n", key, reply.Value)
                return reply.Value
            } else if reply.Err == ErrNoKey {
                //DPrintf("Get ErrNoKey\n")
                return ""
            } else {
                DPrintf("ErrWrongLeader\n")
                ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
            }
        } else {
            DPrintf("Timeout\n")
        }
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    ck.sequenceId++

    args := PutAppendArgs{
        Key : key,
        Value : value,
        Op : op,
        ClientId : ck.id,
        SequenceId : ck.sequenceId,
    }

    //DPrintf("[%v] key:[%v], value:[%v]\n", op, key, value)

    for {
        reply := PutAppendReply{}
        ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

        if ok {
            if reply.Err == ErrWrongLeader {
                DPrintf("ErrWrongLeader")
                ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
            } else {
                //DPrintf("PutAppend success\n")
                break
            }
        } else {
            DPrintf("Timeout\n")
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import (
    "6.824/labgob"
    "6.824/labrpc"
    "6.824/raft"
    "sync"
    "sync/atomic"
    "bytes"
    "time"
)

type Operation struct {
    Op         string
    Key        string
    Value      string
    ClientId   int64
    SequenceId int64
}

type operationReply struct {
    err   Err
    value string
}

var count int = 0

type KVServer struct {
    mu              sync.Mutex
    me              int
    rf              *raft.Raft
    applyCh         chan raft.ApplyMsg
    dead            int32              // set by Kill()
    maxraftstate    int                // snapshot if log grows this big
    data            map[string]string
    operationReplys map[int]chan *operationReply
    requestsId      map[int64]int64
    id              int
}

func (kv *KVServer) saveSnapshot(index int) {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    kv.mu.Lock()
    defer kv.mu.Unlock()

    e.Encode(kv.data)
    e.Encode(kv.requestsId)
    data := w.Bytes()

    kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readSnapshot(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    kv.mu.Lock()
    defer kv.mu.Unlock()

    if d.Decode(&kv.data) != nil ||
        d.Decode(&kv.requestsId) != nil {

    }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    op := Operation{
        Op : "Get",
        Key : args.Key,
        Value : "",
        ClientId : args.ClientId,
        SequenceId : args.SequenceId,
    }

    index, _, isLeader := kv.rf.Start(op)

    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    replyCh := make(chan *operationReply)
    kv.operationReplys[index] = replyCh
    kv.mu.Unlock()

    or := &operationReply{}
    select {
        case or = <-replyCh:
            reply.Err = or.err
            reply.Value = or.value
        case <-time.After(100 * time.Millisecond):
            reply.Err = ErrWrongLeader
    }

    //DPrintf("[Get     return] cli:[%v] seq:[%v]\n", args.ClientId, args.SequenceId)

    kv.mu.Lock()
    delete(kv.operationReplys, index)
    kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    op := Operation{
        Op : args.Op,
        Key : args.Key,
        Value : args.Value,
        ClientId : args.ClientId,
        SequenceId : args.SequenceId,
    }

    index, _, isLeader := kv.rf.Start(op)

    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    replyCh := make(chan *operationReply)
    kv.operationReplys[index] = replyCh
    kv.mu.Unlock()

    or := &operationReply{}
    select {
        case or = <-replyCh:
            reply.Err = or.err
        case <-time.After(100 * time.Millisecond):
            reply.Err = ErrWrongLeader
    }

    //DPrintf("[PutAppend return] cli:[%v] seq:[%v]\n", args.ClientId, args.SequenceId)

    kv.mu.Lock()
    delete(kv.operationReplys, index)
    kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
    atomic.StoreInt32(&kv.dead, 1)
    kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
    z := atomic.LoadInt32(&kv.dead)
    return z == 1
}

func (kv *KVServer) process() {
    for msg := range kv.applyCh {
        if kv.killed() {
            return
        }

        if msg.SnapshotValid {
            kv.readSnapshot(msg.Snapshot)
        } else {
            operation := msg.Command.(Operation)

            kv.mu.Lock()

            if operation.SequenceId <= kv.requestsId[operation.ClientId] {
                kv.mu.Unlock()
                continue
            }

            kv.mu.Unlock()

            or := &operationReply{}
            if operation.Op == "Append" {
                //DPrintf("[Append ] cli:[%v] seq:[%v]\n", operation.ClientId, operation.SequenceId)
                kv.data[operation.Key] += operation.Value
                or.err = OK
            } else if operation.Op == "Put" {
                //DPrintf("[Put    ] cli:[%v] seq:[%v]\n", operation.ClientId, operation.SequenceId)
                kv.data[operation.Key] = operation.Value
                or.err = OK
            } else {
                //DPrintf("[Get    ] cli:[%v] seq:[%v]\n", operation.ClientId, operation.SequenceId)
                value, count := kv.data[operation.Key]

                if count {
                    or.err = OK
                    or.value = value
                } else {
                    or.err = ErrNoKey
                }
            }

            kv.mu.Lock()
            kv.requestsId[operation.ClientId] = operation.SequenceId
            reply, ok := kv.operationReplys[msg.CommandIndex]

            if kv.maxraftstate > -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
                go kv.saveSnapshot(msg.CommandIndex)
            }

            kv.mu.Unlock()

            if ok {
                reply <- or
            }
        }
    }
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Operation{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate
    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.data = make(map[string]string)
    kv.operationReplys = make(map[int]chan *operationReply)
    kv.requestsId = make(map[int64]int64)
    kv.id = count
    count++

    go kv.process()

    return kv
}

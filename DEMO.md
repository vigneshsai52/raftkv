# RaftKV Demo

## 60-Second Demo

1. Start cluster

   docker-compose up

2. Write data

   curl -X POST http://localhost:8080/kv/demo -d '{"value":"hello"}'

3. Read data

   curl http://localhost:8080/kv/demo

4. Kill leader

   docker-compose stop node1

5. Show failover

   curl http://localhost:8081/kv/demo

6. Data still there

   curl http://localhost:8082/kv/demo


## Architecture

- 3-node Raft cluster
- Automatic leader election
- WAL persistence
- Snapshotting
- ~10K ops/sec


---

After saving, run:

   git add .
   git commit -m "Week 7: Production deployment, testing, demo"
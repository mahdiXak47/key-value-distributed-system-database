# DB Node

A database node for the distributed key-value store system. Each DB node is responsible for storing and managing key-value data, supporting efficient in-memory operations using an LSM (Log-Structured Merge) tree structure, and participating in replication and health monitoring.

## Features

- In-memory key-value storage
- LSM-based multi-level MemTable structure
- Write-Ahead Log (WAL) for durability and replication
- Snapshot creation for fast replica synchronization
- HTTP API for Set, Get, and Delete operations
- Health check endpoint
- Ready for integration with replication and controller management

## LSM In-Memory Structure

The DB node uses an in-memory LSM (Log-Structured Merge) tree to efficiently manage data and support fast snapshots and replication. Here's how it works:

### Components
- **Active MemTable (Mutable):** The main in-memory table that receives all new writes (Set/Delete). Fast and thread-safe.
- **Immutable Layers (Read-Only):** When the active MemTable reaches a size threshold or a snapshot is needed, it is frozen and added as an immutable layer. New writes go to a fresh MemTable.
- **Write-Ahead Log (WAL):** Every change is first logged in the WAL before being applied to the MemTable. This ensures durability and allows for replay in case of failure or for replica synchronization.
- **Snapshots:** A snapshot is a combination of all immutable layers and recent WAL entries. It allows new replicas or recovering nodes to quickly catch up to the current state without replaying all historical operations.

### How Snapshots Work
1. **Triggering a Snapshot:** When a new replica joins or a node recovers, a snapshot is triggered. Writes are not blocked during this process.
2. **Flushing MemTable:** The current active MemTable is frozen and becomes an immutable layer. A new MemTable is created for ongoing writes.
3. **Snapshot Composition:** The snapshot consists of all immutable layers (frozen data) and WAL entries since the last checkpoint (recent changes).
4. **Sending to Replicas:** The snapshot is streamed to the replica, which loads the immutable layers and then applies the WAL entries to catch up.
5. **Cleanup:** Once replicas confirm receipt, old immutable layers can be garbage-collected to free memory.

### Advantages
- **No Write Blocking:** Writes continue in a new MemTable while snapshots are created from immutable layers.
- **Fast Recovery:** Replicas can quickly synchronize using snapshots and recent WAL entries.
- **In-Memory Only:** All data is managed in memory for speed, but WAL ensures durability and replayability.

## Architecture

- **MemTable**: Active in-memory table for fast writes
- **Immutable Layers**: Frozen MemTables for snapshotting
- **WAL**: Logs all changes for durability and replay
- **Snapshot**: Combines immutable layers and WAL for replica sync
- **HTTP Server**: Exposes API endpoints for key-value operations and health checks

## API Endpoints

- `POST /set`    - Set a key-value pair
- `GET /get`     - Get a value by key
- `DELETE /delete` - Delete a key-value pair
- `GET /health`  - Health check endpoint

## Usage

1. Make sure you have Go installed
2. Navigate to the db-node directory:
   ```bash
   cd db-node
   ```
3. Run the application:
   ```bash
   go run main.go
   ```
4. The DB node will start on port 9001 by default

## Directory Structure

```
db-node/
├── main.go            # Main application code
├── storage.go         # Key-value storage logic
├── replication.go     # Replication and snapshot logic
├── health.go          # Health check endpoint
└── lsm/               # LSM in-memory structures
    ├── memtable.go
    ├── immutable_layer.go
    ├── wal.go
    └── snapshot.go
```

## Notes

- This node is designed for in-memory operation and does not persist data to disk.
- Snapshots and WAL enable fast recovery and replica synchronization.
- Integrate with the controller and load balancer for full system functionality. 
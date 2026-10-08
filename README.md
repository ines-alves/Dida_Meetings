# DidaMeetings

A fault-tolerant distributed meeting management system built as part of the Design of Distributed Applications (DAD) course at IST Lisbon. The system replicates state across a cluster of servers using a full Multi-Paxos consensus implementation, guaranteeing consistency even under replica crashes or network delays, while supporting live reconfiguration of the acceptor set via Vertical Paxos.

## Tech Stack

**Language & Build**
- Java 22
- Maven 3.8.4

**Communication**
- gRPC (io.grpc)
- Protocol Buffers 3.12 (protoc)

## Features

- Create, manage, and close meetings across a replicated cluster of servers
- Add participants to meetings; meetings auto-close once the maximum participant count is reached
- Assign discussion topics to participants, with causal ordering enforced relative to the `add` that enrolled them
- Dump the full server state (open and closed meetings, participants, topics) from any client
- Elect a new Paxos leader at runtime by issuing a ballot change from the console
- Switch between two cluster configurations (Schedule A: 3 nodes, Schedule B: 6 nodes) via Vertical Paxos reconfiguration
- Debug replicas remotely: freeze, unfreeze, crash, or inject artificial message delays through the console

## Architecture / How It Works

Each server process exposes three gRPC services:

- **MainService** — the client-facing API (`open`, `add`, `topic`, `close`, `dump`). Incoming requests are queued in a `RequestHistory` (pending → in-process → processed) and handed to the `MainLoop` worker thread.
- **PaxosService** — the inter-replica consensus layer (`longPhaseone`, `phasetwo`, `learn`). Phase 2 acceptors notify learners in a forked gRPC context so the accept response is not blocked.
- **MasterService** — the admin control plane (`newballot`, `setdebug`, `activation`). Used by the console to drive leader elections and inject debug modes.

**Consensus flow.** The `MainLoop` worker runs on the current leader. On receiving a new ballot, it executes a "long" Phase 1 — a single prepare message that returns the entire Paxos log from the last committed entry forward, rather than issuing one prepare per slot. After collecting a quorum of promises, it replays any previously accepted values (picking the highest-ballot value per slot, per Paxos safety), then opens an infinite Phase 2 loop that proposes new client requests into successive log slots. When a quorum of acceptors acknowledges Phase 2, they each notify all learners; a learner marks an entry decided once it has seen quorum accepts and executes the command.

**Causal ordering for `topic`.** When a client calls `add`, the server returns the Paxos log index at which that `add` was committed. The client includes this index in the subsequent `topic` call. Each server blocks processing the topic until its own log has reached at least that index, ensuring the participant exists before assigning them a topic.

**Vertical Paxos.** The `ConfigurationScheduler` maps ballot numbers to distinct sets of acceptors, quorum sizes, and leaders. Schedule A uses three replicas (IDs 0–2) with a quorum of 2. Schedule B uses six replicas (IDs 0–5): ballots 0–1 use acceptors {0,1,2} with quorum 2; ballot 2 and above use acceptors {1,2,3,4,5} with quorum 3. The console orchestrates the transition by sending a new ballot, waiting for the leader to complete Phase 1 over the old configuration, and only activating the new ballot once the leader confirms it finished recovering the log.

## Getting Started

### Requirements

- Java 22
- Maven 3.8.4
- Protoc 3.12

### Environment

The project includes a template `setup_env.sh` which you may use as a basis to download all necessary packages. It targets Linux/Ubuntu on Intel x86. Extend it for your distribution/architecture as needed. Once run, activate the environment with:

```bash
source INSTALL_DIR/env.sh
```

### Architecture-specific contract pom

The `contract` module requires a different `pom.xml` depending on CPU architecture:

- ARM/M4 macOS: copy `arm-pom.xml` → `contract/pom.xml`
- Intel/Linux: copy `intel-pom.xml` → `contract/pom.xml`

### Compiling

```bash
mvn clean install
```

Run this from the root directory.

## Usage

### 1. Start the server replicas

Run each of the following in a separate terminal (from the `server` directory). This example starts a 3-replica cluster using Schedule A on base port 8080, with a maximum of 5 participants per meeting:

```bash
mvn exec:java -Dexec.args="8080 0 A 5"
mvn exec:java -Dexec.args="8080 1 A 5"
mvn exec:java -Dexec.args="8080 2 A 5"
```

Each replica binds to `base_port + id` (8080, 8081, 8082).

### 2. Start a client

From the `app` directory:

```bash
mvn exec:java -Dexec.args="1 localhost 8080 A"
```

The client opens an interactive prompt. Example session:

```
app> open 42
Meeting opened with id 42

app> add 42 7
Added participant with id 7 to meeting with id 42

app> topic 42 7 100
Meeting with id 42 participant with id 7 and topic 100

app> close 42
Meeting closed with id 42

app> show
```

`loop` runs an automated workload of random open/add/topic/close operations.

### 3. Start the console

From the `console` directory:

```bash
mvn exec:java -Dexec.args="localhost 8080 A"
```

Example commands:

```
console> ballot 1 1        # ask replica 1 to start ballot 1 (makes it leader)
console> debug freeze 2    # freeze replica 2's main loop
console> debug crash 0     # crash replica 0
console> debug slow-mode-on 1   # add 3-second delay to replica 1's Paxos messages
```

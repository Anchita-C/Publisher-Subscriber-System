```markdown
# Secure Pub/Sub Message Broker

> SSL/TLS encrypted publish-subscribe system over raw TCP sockets — no third-party dependencies.

---

## Overview

A lightweight pub/sub broker where publishers send JSON messages to a central broker, which fans them out in real time to all subscribed clients. All communication is encrypted end-to-end using TLS 1.2+.

| File | Role | Description |
|------|------|-------------|
| `broker.py` | Server | Accepts connections on port 9010, routes messages from publishers to subscribers |
| `pub.py` | Publisher | Connects to broker, creates topics, sends messages |
| `subscriber.py` | Subscriber | Connects to broker, subscribes to topics, receives live messages |
| `server.crt` / `server.key` | SSL Credentials | Self-signed certificate for TLS encryption |

---

## How It Works

**Protocol detection** — the broker shares a single port (9010) for both client types. It peeks at the first byte of each connection to decide how to handle it:
- Publishers send a 10-byte length header starting with a digit → routed to the JSON handler
- Subscribers send plain-text commands starting with a letter → routed to the text handler

**Message flow:**
1. Publisher connects and sends `CREATE` to register a topic
2. Publisher sends `PUBLISH` with a topic name, payload, and timestamp
3. Broker fans the message out to every socket in that topic's subscriber list
4. Subscriber receives it formatted as `topic:data\n`

**Thread model** — each client runs in its own daemon thread. A `threading.Lock` protects the shared `topics` dictionary; a separate `perf_lock` protects performance counters.

---

## Setup

### Prerequisites
- Python 3.8+
- `openssl` (pre-installed on macOS and Linux)
- No third-party Python packages — stdlib only

### Step 1 — Generate the SSL certificate (once)

Run this in the project folder. Both `pub.py` and `subscriber.py` need `server.crt` to verify the broker.

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -days 365 -nodes -subj '/CN=localhost'
```

### Step 2 — Start the broker (Terminal 1)

```bash
python3 broker.py
```

Wait for:
```
[BROKER RUNNING] SSL/TLS on 0.0.0.0:9010
```

### Step 3 — Start the publisher (Terminal 2)

```bash
python3 pub.py
```

### Step 4 — Start the subscriber (Terminal 3)

```bash
python3 subscriber.py
```

---

## Publisher — `pub.py`

On startup, automatically creates four default topics: `sports`, `tech`, `finance`, `weather`.

| Option | Description |
|--------|-------------|
| `1. Manual Publish` | Choose a topic and type a message. Auto-creates the topic if it doesn't exist. |
| `2. Create New Topic` | Register a new topic on the broker without publishing. |
| `3. Stress Test` | Spawns N concurrent threads each sending one message. Reports throughput and latency. |
| `4. Exit` | Closes the SSL connection gracefully. |

### Sample output

```
[CONNECTED] Broker at localhost:9010
[SSL] Protocol: TLSv1.3  |  Cipher: TLS_AES_256_GCM_SHA384
[CREATED] Topic 'sports'
[CREATED] Topic 'tech'
[CREATED] Topic 'finance'
[CREATED] Topic 'weather'

==== Publisher Menu (SSL secured) ====
Active topics : ['sports', 'tech', 'finance', 'weather']
1. Manual Publish
2. Create New Topic
3. Stress Test (performance benchmark)
4. Exit
Choice: 1
Enter topic name (existing or new): sports
Message: Indian Hockey Olympic Team wins Gold!
[SENT] 'sports' → Indian Hockey Olympic Team wins Gold!  (send time: 4.13 ms)
```

---

## Subscriber — `subscriber.py`

Runs a background listener thread to receive messages without blocking the interactive menu.

| Option | Description |
|--------|-------------|
| `1. Subscribe` | Fetches the live topic list and subscribes to a valid topic. Rejects unknown topics. |
| `2. Unsubscribe` | Removes subscription — future messages on that topic stop being delivered. |
| `3. Refresh topics` | Pulls the current topic list from the broker and shows your active subscriptions. |
| `4. Broker stats` | Displays uptime, messages published/delivered, client count, and throughput. |
| `5. Exit` | Unsubscribes from all topics and closes the connection cleanly. |

### Subscribing to a topic

```
==== Subscriber Menu (SSL secured) ====
1. Subscribe to topic
...
Choice: 1
[TOPICS] Available: ['sports', 'tech', 'finance', 'weather']
Available topics : ['sports', 'tech', 'finance', 'weather']
Your subscriptions: []
Enter topic to subscribe : sports
[SUBSCRIBED] → 'sports'
```

### Topic validation

Subscribing to a topic not on the broker is rejected before anything is sent:

```
Enter topic to subscribe : blah
[ERROR] 'blah' is not an available topic. Choose from: ['sports', 'tech', 'finance', 'weather']
```

### Incoming message display

```
╔════════════════════════════════════════════════╗
║  [SPORTS] Indian Hockey Olympic Team wins Gold! ║
║              16:46:36  |  msg #1                ║
╚════════════════════════════════════════════════╝
```

### Unsubscribing

```
Your subscriptions: ['sports', 'tech']
Enter topic to unsubscribe: tech
[UNSUBSCRIBED] ✗ 'tech'
```

### Broker stats

```
[BROKER STATS]
  uptime               442.8s
  published            1
  delivered            1
  clients              2
  throughput           0.00msg/s
  [local] received     1
  [local] rate         0.00 msg/s
```

### Dynamic topic discovery

New topics created by the publisher are immediately visible on refresh:

```
Choice: 3
[INFO] Available topics    : ['sports', 'tech', 'finance', 'weather', 'Crime']
[INFO] Your subscriptions  : ['sports']
```

---

## Stress Test

Option `3` in the publisher menu launches N concurrent threads, each sending one message to a random topic. All threads share one SSL socket protected by a `threading.Lock`.

```
[STRESS TEST] Launching 10 concurrent publish threads...

[STRESS TEST RESULTS]
  Messages sent : 10
  Total time    : 0.0070s
  Throughput    : 1432.87 msg/s
  Avg latency   : 0.6979 ms/msg
```

---

## Security

| Feature | Detail |
|---------|--------|
| TLS minimum version | TLS 1.2 — versions 1.0 and 1.1 are explicitly rejected |
| Cipher suite | Negotiated at runtime; `TLS_AES_256_GCM_SHA384` used in testing (TLS 1.3) |
| Certificate verification | Clients load `server.crt` and verify broker identity during the handshake |
| Private key | `server.key` never leaves the server — only the public cert is distributed |
| Message size limit | Broker rejects payloads over 1 MB to prevent memory exhaustion |

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `FileNotFoundError: server.crt` | Run the `openssl` command in Setup Step 1 |
| `SSL: CERTIFICATE_VERIFY_FAILED` | Cert CN doesn't match `server_hostname`. Regenerate with `/CN=localhost` and set `HOST = "localhost"` |
| `ConnectionRefusedError` | Broker isn't running. Start `broker.py` first and wait for the running log line. |
| `BrokenPipeError` on publish | A subscriber disconnected mid-delivery. The broker handles this automatically. |
```
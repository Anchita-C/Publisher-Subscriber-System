import socket
import json
import time
import random
import threading
import ssl          


HOST        = "localhost"
PORT        = 9010
HEADER_SIZE = 10
FORMAT      = "utf-8"

# Path to broker's certificate — used to verify the server's identity
CERTFILE    = "server.crt"

# ─── Sample Data for Auto-Publish ─────────────────────────────────────────────
DEFAULT_TOPICS = ["sports", "tech", "finance", "weather"]

NEWS_DATA = {
    "sports":  ["India won the match", "Messi scored a hattrick", "Olympics announced new events"],
    "tech":    ["New AI model released", "Quantum computing breakthrough", "New smartphone launched"],
    "finance": ["Stock market hits record high", "Crypto prices surge", "Interest rates increased"],
    "weather": ["Heavy rains expected tomorrow", "Heatwave alert issued", "Cyclone approaching coast"],
}





def connect(host=HOST, port=PORT): #connects to publisher port 9000 (broker)

    # SSLcontext is just the rules ur ssl will follow
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) #protocol tls client is a setting where the socket used must use the highest  ver of tls between both client and server

    # Load the broker's self-signed cert so we can verify it
   
    context.load_verify_locations(CERTFILE)

    # Enforce minimum TLS 1.2 — reject older, insecure handshakes
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Wrap the raw socket with SSL before connecting
    ssl_sock = context.wrap_socket(raw_sock, server_hostname="localhost")

    try:
        ssl_sock.connect((host, port))
        cipher = ssl_sock.cipher() #cipher returns encrytion algo,tls version
        print(f"[CONNECTED] Broker at {host}:{port}")
        print(f"[SSL] Protocol: {cipher[1]}  |  Cipher: {cipher[0]}")
    except ssl.SSLError as e:
        print(f"[SSL ERROR] Could not establish secure connection: {e}")
        raise
    except ConnectionRefusedError:
        print(f"[ERROR] Broker not reachable at {host}:{port}. Is it running?")
        raise

    return ssl_sock




# Serializes a Python dict to JSON and sends it with a 10-byte length header.
# The lock ensures only one thread writes to the socket at a time.

def send(sock, data, lock):
    msg    = json.dumps(data).encode(FORMAT) # sends data dict to json then to bytes via encode to broker
    header = f"{len(msg):<{HEADER_SIZE}}".encode(FORMAT)   # e.g. "42        " #makes it so that the header is fixed size
    with lock:
        try:
            sock.sendall(header + msg)   # sendall() guarantees all bytes are sent #only one thread will send msg
            return True 
        except ssl.SSLError as e:
            print(f"[SSL ERROR] Send failed: {e}")
            return False
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            print(f"[CONN ERROR] Send failed: {e}")
            return False




def create_topic(sock, topic, lock):
    send(sock, {"cmd": "CREATE", "topic": topic}, lock)
    print(f"[CREATED] Topic '{topic}'")


def publish(sock, topic, message, lock):
   
    send(sock, {
        "cmd":       "PUBLISH",
        "topic":     topic,
        "data":      message,
        "timestamp": time.time()    
    }, lock)



# random_news()
# Picks a random topic and a matching headline for auto-publish mode.

def random_news(topics):
    topic = random.choice(topics)
    msg   = random.choice(NEWS_DATA.get(topic, [f"Update on {topic}"]))
    return topic, msg



def stress_test(sock, lock, topics, n=1000):
    print(f"\n[STRESS TEST] Launching {n} concurrent publish threads...")
    threads = []
    start   = time.time()

    # Create all threads first (don't start yet)
    for _ in range(n):
        topic, msg = random_news(topics)
        t = threading.Thread(target=publish, args=(sock, topic, msg, lock))
        threads.append(t)

    # Start all threads as close together as possible
    for t in threads:
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join() # waits until each thread is completed

    elapsed    = time.time() - start
    throughput = n / elapsed if elapsed > 0 else float("inf")

    print(f"\n[STRESS TEST RESULTS]")
    print(f"  Messages sent : {n}")
    print(f"  Total time    : {elapsed:.4f}s")
    print(f"  Throughput    : {throughput:.2f} msg/s")
    print(f"  Avg latency   : {(elapsed / n) * 1000:.4f} ms/msg")


# ─────────────────────────────────────────────────────────────────────────────
# main()
# Interactive CLI menu for the publisher.
# ─────────────────────────────────────────────────────────────────────────────
def main():
    sock = connect()
    lock = threading.Lock() # lets say pub sends 2 msgs the msgs might get interleaved

    # Register default topics at startup
    active_topics = list(DEFAULT_TOPICS)
    for t in active_topics:
        create_topic(sock, t, lock)

    while True:
        print("\n==== Publisher Menu (SSL secured) ====")
        print(f"Active topics : {active_topics}")
        print("1. Manual Publish")
        print("2. Create New Topic")
        print("3. Stress Test (performance benchmark)")
        print("4. Exit")

        choice = input("Choice: ").strip()

        if choice == "1":
            topic = input("Enter topic name (existing or new): ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
                continue
            if topic not in active_topics: # if not in topic then create
                create_topic(sock, topic, lock)
                active_topics.append(topic)
                print(f"[INFO] New topic '{topic}' created.")
            msg = input("Message: ").strip()
            if msg:
                t_send = time.time()
                publish(sock, topic, msg, lock)
                print(f"[SENT] '{topic}' → {msg}  (send time: {(time.time()-t_send)*1000:.2f} ms)")

        elif choice == "2":
            topic = input("New topic name: ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
            elif topic in active_topics:
                print(f"[INFO] Topic '{topic}' already exists.")
            else:
                create_topic(sock, topic, lock)
                active_topics.append(topic)
                print(f"[CREATED] Topic '{topic}' is now active.")

        elif choice == "3":
            try:
                n = int(input("Number of messages to send: ").strip())
            except ValueError:
                print("[ERROR] Please enter a valid integer.")
                continue
            stress_test(sock, lock, active_topics, n)

        elif choice == "4":
            print("[EXIT] Closing secure connection...")
            try:
                sock.close()
            except Exception:
                pass
            break

        else:
            print("[!] Invalid choice. Enter 1-4.")


if __name__ == "__main__":
    main()
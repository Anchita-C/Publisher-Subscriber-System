import socket
import threading
import time
import ssl     # SSL/TLS for secure communication with the broker

HOST = "localhost"
PORT     = 9010
FORMAT   = "utf-8"
CERTFILE = "server.crt" # Broker's certificate for identity verification

available_topics = []
topics_lock      = threading.Lock() #mutex lock created so that one client can access shared resource at a time 

msg_count      = 0 #counter for number of messages received
session_start  = time.time()
msg_count_lock = threading.Lock() 


def connect(host=HOST, port=PORT):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) #contains all security rules
    #transport layer security

    # Trust the broker's self-signed cert
    context.load_verify_locations(CERTFILE)

    # Enforce minimum TLS 1.2
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#create socket
    #this says apply all rules in "context" to this scket and connects to broker at localhost:9000
    ssl_sock = context.wrap_socket(raw_sock, server_hostname="localhost")

    try:
        ssl_sock.connect((host, port))
        cipher = ssl_sock.cipher()#cipher has details about the encryption used 
        print(f"[CONNECTED] Broker at {host}:{port}")
        print(f"[SSL] Protocol: {cipher[1]}  |  Cipher: {cipher[0]}\n")#cipher[0]:encryption algorithm 
    except ssl.SSLError as e:
        print(f"[SSL ERROR] Handshake failed: {e}")
        print("  Make sure the broker is running and server.crt is present.")
        raise
    except ConnectionRefusedError:
        print(f"[ERROR] Cannot reach broker at {host}:{port}. Is it running?")
        raise

    return ssl_sock


#the messages given to the function are encoded into a computer understandable format (bytes) 
#then it sends the message through the network (sendall)
def send_command(sock, command: str):
    try:
        sock.sendall((command + "\n").encode(FORMAT))
    except ssl.SSLError as e:
        print(f"[SSL ERROR] Could not send command: {e}")
    except BrokenPipeError:
        print("[ERROR] Connection to broker lost")

#creates a command string 'SUBSCRIBE sports' and sends it to broker using send_command 
# the broker adds this client to the "sports subscriber list"
def subscribe(sock, topic: str):
    send_command(sock, f"SUBSCRIBE {topic}")
    print(f"[SUBSCRIBED] → '{topic}'")

#reates a command string 'UNSUBSCRIBE sports' and sends it to broker using send_command 
# the broker then removes this client from the "sports subscriber list"
def unsubscribe(sock, topic: str):
    send_command(sock, f"UNSUBSCRIBE {topic}")
    print(f"[UNSUBSCRIBED] ✗ '{topic}'")


def request_topic_list(sock):
    send_command(sock, "LIST_TOPICS")


def request_stats(sock):
    #Ask the broker for performance statistics.
    send_command(sock, "STATS")

#Background thread: continuously listen for incoming messages.
def receive_messages(sock):
    global available_topics, msg_count

    print("[LISTENING] Waiting for messages...\n")
    buffer = ""   # Accumulate partial data between recv() calls

    while True:
        try:
            # gets up to 4096 bytes from socket. messages are then changed to human readable format from byte.
            chunk = sock.recv(4096).decode(FORMAT)
            #if there is a break in the connection it is detected and exits the loop
            if not chunk:
                print("[DISCONNECTED] Broker closed the connection.")
                break

            #storing incoming data from socket
            buffer += chunk

            # Process every complete newline-terminated line in the buffer
            while "\n" in buffer: #until we find  newline from broker
                line, buffer = buffer.split("\n", 1) #extracts the first line from buffer
                line = line.strip()
                if not line:
                    continue

                if line.startswith("TOPICS:"):
                    raw_topics = line[len("TOPICS:"):] #simply removes the word "TOPICS"
                    new_topics = [t for t in raw_topics.split(",") if t] #splits the remining topics
                    with topics_lock:
                        available_topics = new_topics #update the topics 
                    print(f"\n[TOPICS] Available: {available_topics}")
                    print("Choice: ", end="", flush=True)

                elif line.startswith("STATS:"):
                    raw_stats = line[len("STATS:"):]
                    print("\n[BROKER STATS]")
                    for pair in raw_stats.split(","):
                        if "=" in pair:
                            key, val = pair.split("=", 1)
                            print(f"  {key:<20} {val}")
                    # Local subscriber stats
                    with msg_count_lock:
                        count   = msg_count
                        elapsed = time.time() - session_start
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"  {'[local] received':<20} {count}")
                    print(f"  {'[local] rate':<20} {rate:.2f} msg/s")
                    print("Choice: ", end="", flush=True)

                elif ":" in line: #splits the info into topic, message. uses only the first colon
                    topic, message = line.split(":", 1)
                    timestamp      = time.strftime("%H:%M:%S")

                    with msg_count_lock:
                        msg_count += 1
                        count = msg_count

                    # formatting the notification that the broker sends
                    content = f"[{topic.upper()}] {message}"
                    width   = max(len(content) + 4, 30)

                    width = 50
                    PURPLE = "\033[95m"
                    RESET = "\033[0m"

                    print("\n" + PURPLE + "╔" + "═" * (width - 2) + "╗" + RESET)

                    print(PURPLE + "║ " + RESET + f"{content.center(width - 4)}" + PURPLE + " ║" + RESET)

                    print(PURPLE + "║ " + RESET + f"{timestamp}  |  msg #{count}".center(width - 4) + PURPLE + " ║" + RESET)

                    print(PURPLE + "╚" + "═" * (width - 2) + "╝" + RESET + "\n")
                    #end ="" cursor stays in the same line after printing and 
                    # When a message arrives and prints, flush=True ensures the input prompt is immediately shown again without delay.
                    print("Choice: ", end="", flush=True) # Re-prompt

        except ssl.SSLError as e:
            print(f"\n[SSL ERROR] {e}")
            break
        except UnicodeDecodeError:
            print("[WARNING] Received non-UTF-8 data — skipping")
            buffer = ""   # Discard corrupted buffer
            continue
        #if anything goes wrong in the try block it goes here and print error and stop the loop
        except Exception as e:
            print(f"\n[ERROR] {e}")
            break

    print("[LISTENER STOPPED]")

def show_menu():
    print("\n==== Subscriber Menu (SSL secured) ====")
    print("1. Subscribe to topic")
    print("2. Unsubscribe from topic")
    print("3. Show / Refresh available topics")
    print("4. View broker stats")
    print("5. Exit")

def main():
    global available_topics

    sock              = connect()
    #this is a list to track user subscriptions it prevents duplicate subscriptions
    subscribed_topics = []

    # Fetch topic list immediately on startup
    request_topic_list(sock)

    # this runs receive_messages() in the background which helps with receiving of messages without blocking the user input
    #daemon = true ; Thread automatically exits when main program exits
    listener = threading.Thread(target=receive_messages, args=(sock,), daemon=True)
    listener.start()

    # Give the listener a moment to receive the initial topic list
    time.sleep(0.3)

    #this is the menu loop where the user interacts by using subscribe unsubscribe etc
    while True:
        show_menu()
        choice = input("Choice: ").strip()

        if choice == "1":
            # Refresh topic list before showing options
            request_topic_list(sock)
            time.sleep(0.2)   # brief wait for broker response

            with topics_lock:
                current_topics = list(available_topics)

            print(f"Available topics : {current_topics}")
            print(f"Your subscriptions: {subscribed_topics}")
            topic = input("Enter topic to subscribe : ").strip()

            if not topic:
                print("[ERROR] Topic name cannot be empty.")
                continue

            if topic not in current_topics:
                print(f"[ERROR] '{topic}' is not an available topic. Choose from: {current_topics}")
                continue

            if topic in subscribed_topics:
                print(f"[INFO] Already subscribed to '{topic}'")
            else:
                subscribe(sock, topic)
                subscribed_topics.append(topic)
                # Add to local available list if it's brand new
                with topics_lock:
                    if topic not in available_topics:
                        available_topics.append(topic)

        elif choice == "2":
            if not subscribed_topics:
                print("[INFO] You have no active subscriptions.")
            else:
                print(f"Your subscriptions: {subscribed_topics}")
                topic = input("Enter topic to unsubscribe: ").strip()
                if topic in subscribed_topics:
                    unsubscribe(sock, topic)
                    subscribed_topics.remove(topic)
                else:
                    print(f"[INFO] You are not subscribed to '{topic}'")

        elif choice == "3":
            # Explicitly refresh from broker
            request_topic_list(sock)
            time.sleep(0.2)
            with topics_lock:
                current_topics = list(available_topics)
            print(f"[INFO] Available topics    : {current_topics}")
            print(f"[INFO] Your subscriptions  : {subscribed_topics}")

        elif choice == "4":
            # Request live stats from broker, displayed by listener thread
            request_stats(sock)
            time.sleep(0.3)   # Allow listener thread to print the response

        elif choice == "5":
            print("[EXIT] Unsubscribing and closing secure connection...")
            #loops through every topic that has been subscribed to and unsubscribes from them
            for topic in subscribed_topics:
                unsubscribe(sock, topic)
            try:
                sock.close()
            except Exception:
                pass
            break

        else:
            print("[!] Invalid choice. Enter 1–5.")


if __name__ == "__main__":
    main()
import sqlite3
import time
import multiprocessing
import threading
from eth_keys import keys
from eth_utils import keccak

def log_status(message):
    print(f"[STATUS] {message}")

def private_key_to_address(private_key_hex):
    private_key_bytes = bytes.fromhex(private_key_hex)
    private_key = keys.PrivateKey(private_key_bytes)
    public_key = private_key.public_key
    address = keccak(public_key.to_bytes())[12:]
    return '0x' + address.hex()

def save_to_database(batch_data, db_name="wallets.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS wallets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        private_key TEXT UNIQUE,
                        address TEXT UNIQUE)''')
    try:
        cursor.executemany("INSERT OR IGNORE INTO wallets (private_key, address) VALUES (?, ?)", batch_data)
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # Ignore duplicate entries
    conn.close()

def process_keys(start_key_hex, num_keys, batch_size=1000):
    private_key_int = int(start_key_hex, 16)
    batch_data = []
    for _ in range(num_keys):
        private_key_hex = format(private_key_int, '064x')
        eth_address = private_key_to_address(private_key_hex)
        batch_data.append((private_key_hex, eth_address))
        private_key_int += 1
        if len(batch_data) >= batch_size:
            save_to_database(batch_data)
            batch_data = []
    if batch_data:
        save_to_database(batch_data)

def parallel_key_generation(start_key_hex, total_keys=1000000, num_workers=4):
    keys_per_worker = total_keys // num_workers
    processes = []
    for i in range(num_workers):
        start_key = format(int(start_key_hex, 16) + (i * keys_per_worker), '064x')
        p = multiprocessing.Process(target=process_keys, args=(start_key, keys_per_worker))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

def threaded_status_log():
    while True:
        log_status("Script is running...")
        time.sleep(10)

# Start parallel processing
log_status("Starting optimized key generation.")
status_thread = threading.Thread(target=threaded_status_log, daemon=True)
status_thread.start()
parallel_key_generation("0000000000000000000000000000000000000000000000000000000000000001", total_keys=10000000, num_workers=8)
log_status("Script completed.")

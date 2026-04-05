import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

CONTAINER_NAME = "myMariaDB"
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "part3Flight"
DUMP_PATH = r"D:\OneDrive - Fondation EPF\Ubuntu\4A\S8\Data_Models\Lab 1 Robust DBMS for transactional\benchmarking.sql"

NUM_THREADS = 8


def reset_db():
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            CONTAINER_NAME,
            "mariadb",
            f"-u{DB_USER}",
            f"-p{DB_PASSWORD}",
            "-e",
            f"DROP DATABASE IF EXISTS {DB_NAME}; CREATE DATABASE {DB_NAME};",
        ],
        capture_output=True,
        text=True,
    )


def split_sql_file(file_path, n_chunks):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    queries = content.split(";")
    chunk_size = len(queries) // n_chunks

    chunks = []
    for i in range(n_chunks):
        start = i * chunk_size
        end = None if i == n_chunks - 1 else (i + 1) * chunk_size
        chunk = ";".join(queries[start:end])
        chunks.append(chunk)

    return chunks


def import_chunk(args):
    chunk_id, sql_chunk = args

    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            CONTAINER_NAME,
            "mariadb",
            f"-u{DB_USER}",
            f"-p{DB_PASSWORD}",
            DB_NAME,
        ],
        input=sql_chunk,
        capture_output=True,
        text=True,
    )

    return {
        "chunk_id": chunk_id,
        "returncode": result.returncode,
        "error": result.stderr[:300],
    }


if __name__ == "__main__":
    print("MULTI-THREAD IMPORT (8 threads)")

    reset_db()

    print("Splitting SQL file...")
    chunks = split_sql_file(DUMP_PATH, NUM_THREADS)

    start = time.time()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = list(executor.map(import_chunk, enumerate(chunks)))

    elapsed = time.time() - start

    print(f"\nImport with {NUM_THREADS} threads done in {elapsed:.2f}s\n")

    # Debug print the results to check if any thread failed and print the error message
    failed = [r for r in results if r["returncode"] != 0]

    if not failed:
        print("All threads completed successfully ")
    else:
        print(f"{len(failed)} threads failed \n")

        for f in failed:
            print(f"Chunk {f['chunk_id']} FAILED:")
            print(f"{f['error']}\n")

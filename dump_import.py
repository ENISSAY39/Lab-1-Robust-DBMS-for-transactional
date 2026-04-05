import subprocess
import time
import os

CONTAINER_NAME = "myMariaDB"
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "part3Flight"
DUMP_PATH = r"D:\OneDrive - Fondation EPF\Ubuntu\4A\S8\Data_Models\Lab 1 Robust DBMS for transactional\benchmarking.sql"
OUTPUT_DIR = r"D:\OneDrive - Fondation EPF\Ubuntu\4A\S8\Data_Models\Lab 1 Robust DBMS for transactional"


def reset_db(db_name):
    print(f"Resetting database '{db_name}'...")
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
            f"DROP DATABASE IF EXISTS {db_name}; CREATE DATABASE {db_name};",
        ],
        capture_output=True,
        text=True,
    )
    print("Database reset done.")


def import_dump(dump_path, db_name):
    print(f"Importing '{dump_path}' into '{db_name}'...")
    start = time.time()
    with open(dump_path, "r", encoding="utf-8") as f:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                CONTAINER_NAME,
                "mariadb",
                f"-u{DB_USER}",
                f"-p{DB_PASSWORD}",
                db_name,
            ],
            stdin=f,
            capture_output=True,
            text=True,
        )
    elapsed = time.time() - start
    if result.returncode == 0:
        print(f"Import successful in {elapsed:.2f}s")
    else:
        print(f"Import FAILED: {result.stderr[:200]}")
    return elapsed


def export_dump(db_name, output_path):
    print(f"Exporting '{db_name}' to '{output_path}'...")
    start = time.time()
    with open(output_path, "w", encoding="utf-8") as f:
        result = subprocess.run(
            [
                "docker",
                "exec",
                CONTAINER_NAME,
                "mariadb-dump",
                f"-u{DB_USER}",
                f"-p{DB_PASSWORD}",
                db_name,
            ],
            stdout=f,
            capture_output=False,
            text=True,
        )
    elapsed = time.time() - start
    print(f"Export done in {elapsed:.2f}s")
    return elapsed


def compare_dumps(files):
    print("\n--- Comparing dumps ---")
    sizes = []
    for f in files:
        size = os.path.getsize(f)
        sizes.append(size)
        print(f"{os.path.basename(f)} : {size} bytes")

    if len(set(sizes)) == 1:
        print("All dumps are identical in size -> RESILIENT")
    else:
        print("Dumps differ in size -> NOT resilient!")


if __name__ == "__main__":
    exported_files = []
    timings = []

    for i in range(1, 6):
        print(f"\n{'=' * 40}")
        print(f"ITERATION {i}/5")
        print(f"{'=' * 40}")

        reset_db(DB_NAME)
        t_import = import_dump(DUMP_PATH, DB_NAME)

        output_path = os.path.join(OUTPUT_DIR, f"dump_export_{i}.sql")
        t_export = export_dump(DB_NAME, output_path)

        exported_files.append(output_path)
        timings.append((i, t_import, t_export))

    print("\n--- Timing summary ---")
    for i, t_imp, t_exp in timings:
        print(f"Iteration {i} : import {t_imp:.2f}s | export {t_exp:.2f}s")

    compare_dumps(exported_files)

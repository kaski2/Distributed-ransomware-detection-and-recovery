import argparse
import random
import string
import time
from pathlib import Path


NOTE_NAMES = [
    "READ_ME_TO_DECRYPT.txt",
    "RECOVER_FILES_NOW.txt",
    "HOW_TO_RESTORE_DATA.txt",
]
NOTE_TEXTS = [
    "Your files are encrypted. Read this note to decrypt your data.",
    "To recover your files, contact support with your key.",
    "Restore access instructions are in this file.",
]
EXTENSIONS = [".txt", ".log", ".cfg", ".json", ".md"]


def random_text(size=256):
    alphabet = string.ascii_letters + string.digits + " _-"
    return "".join(random.choice(alphabet) for _ in range(size))


def create_file(target_dir, files):
    name = f"file_{int(time.time() * 1000)}_{random.randint(1000, 9999)}{random.choice(EXTENSIONS)}"
    path = target_dir / name
    path.write_text(random_text(512), encoding="utf-8", errors="ignore")
    files.append(path)
    return "created"


def modify_file(target_dir, files):
    if not files:
        return create_file(target_dir, files)
    path = random.choice(files)
    with open(path, "a", encoding="utf-8", errors="ignore") as handle:
        handle.write("\n" + random_text(128))
    return "modified"


def rename_file(target_dir, files):
    if not files:
        return create_file(target_dir, files)
    src = random.choice(files)
    dst = src.with_name(f"{src.stem}_{random.randint(100, 999)}{src.suffix}")
    src.rename(dst)
    files.remove(src)
    files.append(dst)
    return "renamed"


def delete_file(target_dir, files):
    if not files:
        return create_file(target_dir, files)
    path = random.choice(files)
    path.unlink(missing_ok=True)
    files.remove(path)
    return "deleted"


def drop_note(target_dir, files):
    name = f"{int(time.time())}_{random.choice(NOTE_NAMES)}"
    path = target_dir / name
    path.write_text(random.choice(NOTE_TEXTS), encoding="utf-8", errors="ignore")
    files.append(path)
    return "ransom_notes"


# set this list if you want to hardcode directories instead of passing
# them on the command line.  The value should be a list of strings that
# represent directories which already exist on disk.  When this list is
# non-empty the CLI argument `--targets` is ignored.
HARD_CODED_TARGETS = [
    
]


def parse_args():
    parser = argparse.ArgumentParser(description="Simple durability test for file-monitor pipeline")
    parser.add_argument("--targets", required=not bool(HARD_CODED_TARGETS),
                        help="Comma-separated monitored dirs")
    parser.add_argument("--duration-seconds", type=int, default=360)
    parser.add_argument("--ops-per-second", type=int, default=8)
    parser.add_argument("--ransom-every-n-ops", type=int, default=6)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main():
    args = parse_args()
    random.seed(args.seed)

    # determine monitored directories -- either use the hardcoded list or
    # fall back to the CLI argument.  This makes it easy to quickly tweak the
    # directories inside the source without needing to remember the
    # invocation pattern.
    if HARD_CODED_TARGETS:
        targets = [Path(p).resolve() for p in HARD_CODED_TARGETS]
    else:
        targets = [Path(part.strip()).resolve() for part in args.targets.split(",") if part.strip()]
        if not targets:
            raise ValueError("No target directories provided")

    for target in targets:
        if not target.exists():
            raise FileNotFoundError(f"Target path does not exist: {target}")

    workdirs = []
    for target in targets:
        workdir = target / "durability_test_payload"
        workdir.mkdir(parents=True, exist_ok=True)
        workdirs.append(workdir)

    file_pool = {workdir: [] for workdir in workdirs}
    counters = {"created": 0, "modified": 0, "renamed": 0, "deleted": 0, "ransom_notes": 0, "errors": 0}

    print("Starting durability test")
    print(f"Targets: {', '.join(str(w) for w in workdirs)}")
    print(f"Duration: {args.duration_seconds}s")
    print(f"Ops/sec: {args.ops_per_second}")
    print(f"Ransom every N ops: {args.ransom_every_n_ops}")
    if args.duration_seconds < 330:
        print("[WARN] monitor loop polls every 300s; use >=330s for reliable end-to-end alerts")

    operations = [create_file, modify_file, rename_file, delete_file]
    delay = 1.0 / max(1, args.ops_per_second)
    deadline = time.time() + args.duration_seconds
    op_index = 0

    while time.time() < deadline:
        target = random.choice(workdirs)
        files = file_pool[target]
        try:
            if op_index % max(1, args.ransom_every_n_ops) == 0:
                action = drop_note(target, files)
            else:
                action = random.choice(operations)(target, files)
            counters[action] += 1
        except Exception:
            counters["errors"] += 1

        op_index += 1
        time.sleep(delay)

    print("\nFinished. Summary:")
    for key, value in counters.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()

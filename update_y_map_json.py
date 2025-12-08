import hashlib
from datetime import datetime
from pathlib import Path

import requests

REMOTE_URL = "https://daodemo.tech/api/channels/with-map?source=inferred"
LOCAL_PATH = Path("y_map_channels.json")


def calculate_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def create_backup(path: Path, content: bytes) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_name(f"{path.name}.bak_{timestamp}")
    backup_path.write_bytes(content)
    return backup_path


def main() -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    local_bytes: bytes | None = None
    old_hash: str | None = None

    if LOCAL_PATH.exists():
        local_bytes = LOCAL_PATH.read_bytes()
        old_hash = calculate_sha256(local_bytes)

    response = requests.get(REMOTE_URL, timeout=60)
    response.raise_for_status()
    new_bytes = response.content
    new_hash = calculate_sha256(new_bytes)

    if old_hash == new_hash:
        print(f"[{now}] Змін не виявлено. Локальний файл актуальний.")
        return 0

    backup_path: Path | None = None
    if local_bytes is not None:
        backup_path = create_backup(LOCAL_PATH, local_bytes)

    LOCAL_PATH.write_bytes(new_bytes)

    print(f"[{now}] Файл оновлено.")
    if old_hash is not None:
        print(f"Старий хеш: {old_hash}")
    print(f"Новий хеш: {new_hash}")
    if backup_path is not None:
        print(f"Шлях до бекапу: {backup_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

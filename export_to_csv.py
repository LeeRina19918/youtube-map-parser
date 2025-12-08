import csv
import json
from pathlib import Path

COLUMNS = [
    "channelName",
    "originalUrl",
    "subscribersCount",
    "viewsCount",
    "videosCount",
    "channelCategories",
    "definedCategories",
    "clusterName",
    "inferredClusterName",
]


def load_channels(json_path: Path) -> list[dict]:
    with json_path.open("r", encoding="utf-8") as json_file:
        return json.load(json_file)


def format_list(value) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def extract_row(channel: dict) -> dict:
    statistics = channel.get("statistic", {}) or {}
    return {
        "channelName": channel.get("channelName", ""),
        "originalUrl": channel.get("originalUrl", ""),
        "subscribersCount": statistics.get("subscribersCount", ""),
        "viewsCount": statistics.get("viewsCount", ""),
        "videosCount": statistics.get("videosCount", ""),
        "channelCategories": format_list(channel.get("channelCategories", [])),
        "definedCategories": format_list(channel.get("definedCategories", [])),
        "clusterName": channel.get("clusterName", ""),
        "inferredClusterName": channel.get("inferredClusterName", ""),
    }


def write_csv(rows: list[dict], csv_path: Path) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    json_path = repo_root / "y_map_channels.json"
    csv_path = repo_root / "channels_all.csv"

    channels = load_channels(json_path)
    rows = [extract_row(channel) for channel in channels]
    write_csv(rows, csv_path)

    print(f"Готово. Записано {len(rows)} каналів.")


if __name__ == "__main__":
    main()

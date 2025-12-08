from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

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


def load_channels(json_path: Path) -> list[dict[str, Any]]:
    with json_path.open("r", encoding="utf-8") as json_file:
        return json.load(json_file)


def format_list(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def extract_row(channel: dict[str, Any]) -> dict[str, str]:
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


def slugify(text: str) -> str:
    """Зробити з назви кластера безпечний шматок для назви файлу."""
    text = text.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "", text)
    return text or "filtered"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Фільтрація каналів за кластером і мінімальною кількістю підписників"
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="Назва кластера (як у полі clusterName, напр. 'Кулінарні шедеври вдома')",
    )
    parser.add_argument(
        "--min-subscribers",
        type=int,
        default=0,
        help="Мінімальна кількість підписників (ціле число)",
    )
    parser.add_argument(
        "--output",
        help="Назва вихідного CSV (необов'язково)",
    )
    return parser.parse_args()


def write_csv(rows: list[dict[str, str]], csv_path: Path) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()

    repo_root = Path(__file__).resolve().parent
    json_path = repo_root / "y_map_channels.json"

    if args.output:
        csv_path = repo_root / args.output
    else:
        slug = slugify(args.cluster)
        csv_path = repo_root / f"filtered_{slug}_min{args.min_subscribers}.csv"

    channels = load_channels(json_path)

    cluster_name = args.cluster
    min_subs = int(args.min_subscribers)

    filtered_rows: list[dict[str, str]] = []

    for channel in channels:
        # фільтр за кластером
        if channel.get("clusterName") != cluster_name:
            continue

        # фільтр за кількістю підписників
        statistics = channel.get("statistic", {}) or {}
        subs_raw = statistics.get("subscribersCount", "0")
        try:
            subs = int(subs_raw)
        except (TypeError, ValueError):
            subs = 0

        if subs < min_subs:
            continue

        filtered_rows.append(extract_row(channel))

    write_csv(filtered_rows, csv_path)

    print(
        f"Готово. Знайдено {len(filtered_rows)} канал(ів) "
        f"у кластері '{cluster_name}' з ≥ {min_subs} підписниками."
    )
    print(f"Результат записано у файл: {csv_path.name}")


if __name__ == "__main__":
    main()

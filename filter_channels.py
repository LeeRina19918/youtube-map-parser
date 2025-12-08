"""CLI для фільтрації каналів із y_map_channels.json."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Фільтрація каналів за кластером, категоріями та іншими параметрами.",
    )
    parser.add_argument(
        "--json-path",
        default="y_map_channels.json",
        type=str,
        help="Шлях до JSON-файлу з каналами (за замовчуванням: y_map_channels.json)",
    )
    parser.add_argument(
        "--cluster",
        type=str,
        help="Назва кластера (clusterName). Можна вказати кілька через кому.",
    )
    parser.add_argument(
        "--inferred-cluster",
        type=str,
        help="Назва inferredClusterName. Можна вказати кілька через кому.",
    )
    parser.add_argument(
        "--min-subscribers",
        type=int,
        default=0,
        help="Мінімальна кількість підписників (ціле число, за замовчуванням 0)",
    )
    parser.add_argument(
        "--max-subscribers",
        type=int,
        default=None,
        help="Максимальна кількість підписників (якщо не задано — без обмеження)",
    )
    parser.add_argument(
        "--category",
        type=str,
        help="Категорії з definedCategories. Можна вказати кілька через кому; співпадіння за входженням без урахування регістру.",
    )
    parser.add_argument(
        "--keyword",
        type=str,
        help="Ключове слово для пошуку у lastVideoTitles (без урахування регістру)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Максимальна кількість каналів у результаті (<=0 — без обмеження)",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        help="Шлях до CSV-файлу для збереження результатів фільтрації",
    )
    return parser.parse_args()


def load_channels(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Файл {path} не знайдено.")

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Не вдалося прочитати JSON: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError("Очікується масив об'єктів у JSON.")

    return data


def split_arg_values(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    values = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return values or None


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.strip().lower()


def parse_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def category_matches(channel_categories: Iterable[Any], patterns: List[str]) -> bool:
    for cat in channel_categories:
        cat_text = str(cat).lower()
        for pattern in patterns:
            if pattern in cat_text:
                return True
    return False


def keyword_matches(titles: Iterable[Any], keyword: str) -> bool:
    keyword_lower = keyword.lower()
    for title in titles:
        if keyword_lower in str(title).lower():
            return True
    return False


def filter_channels(data: List[Dict[str, Any]], args: argparse.Namespace) -> List[Dict[str, Any]]:
    clusters = split_arg_values(args.cluster)
    inferred_clusters = split_arg_values(args.inferred_cluster)
    categories = split_arg_values(args.category)
    keyword = args.keyword.lower() if args.keyword else None

    filtered = []
    for item in data:
        subscribers = parse_int(item.get("statistic", {}).get("subscribersCount"))
        cluster_name = normalize_text(item.get("clusterName"))
        inferred_cluster_name = normalize_text(item.get("inferredClusterName"))

        if clusters is not None and (cluster_name is None or cluster_name not in clusters):
            continue
        if inferred_clusters is not None and (
            inferred_cluster_name is None or inferred_cluster_name not in inferred_clusters
        ):
            continue
        if subscribers < args.min_subscribers:
            continue
        if args.max_subscribers is not None and subscribers > args.max_subscribers:
            continue

        if categories is not None:
            defined_categories = item.get("definedCategories") or []
            if not category_matches(defined_categories, categories):
                continue

        if keyword is not None:
            titles = item.get("lastVideoTitles") or []
            if not keyword_matches(titles, keyword):
                continue

        item_copy = dict(item)
        item_copy.setdefault("statistic", {})
        item_copy["statistic"] = dict(item_copy["statistic"])
        item_copy["statistic"]["subscribersCount"] = subscribers
        filtered.append(item_copy)

    filtered.sort(key=lambda x: x.get("statistic", {}).get("subscribersCount", 0), reverse=True)
    return filtered


def print_results(channels: List[Dict[str, Any]], total_before_limit: int) -> None:
    for channel in channels:
        statistic = channel.get("statistic", {})
        subscribers = statistic.get("subscribersCount", 0)
        line = " | ".join(
            [
                str(channel.get("channelName", "(без назви)")),
                f"{subscribers} підписників",
                str(channel.get("clusterName", "")),
                str(channel.get("originalUrl", "")),
            ]
        )
        print(line)
    print(f"Знайдено каналів: {total_before_limit}")
    if total_before_limit != len(channels):
        print(f"Показано: {len(channels)}")


def save_to_csv(channels: List[Dict[str, Any]], output_path: Path) -> None:
    columns = [
        "channelName",
        "originalUrl",
        "subscribersCount",
        "viewsCount",
        "videosCount",
        "clusterName",
        "inferredClusterName",
        "definedCategories",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for channel in channels:
            statistic = channel.get("statistic", {})
            defined_categories = channel.get("definedCategories") or []
            writer.writerow(
                {
                    "channelName": channel.get("channelName", ""),
                    "originalUrl": channel.get("originalUrl", ""),
                    "subscribersCount": statistic.get("subscribersCount", ""),
                    "viewsCount": statistic.get("viewsCount", ""),
                    "videosCount": statistic.get("videosCount", ""),
                    "clusterName": channel.get("clusterName", ""),
                    "inferredClusterName": channel.get("inferredClusterName", ""),
                    "definedCategories": "; ".join(map(str, defined_categories)),
                }
            )
    print(f"Дані збережено у {output_path}")


def main() -> int:
    args = parse_args()
    json_path = Path(args.json_path)

    try:
        data = load_channels(json_path)
    except FileNotFoundError as exc:
        print(str(exc))
        return 1
    except ValueError as exc:
        print(f"Помилка читання JSON: {exc}")
        return 1

    filtered = filter_channels(data, args)
    total_before_limit = len(filtered)

    limit = args.limit
    if limit is not None and limit > 0:
        filtered = filtered[:limit]

    print_results(filtered, total_before_limit)

    if args.output_csv:
        save_to_csv(filtered, Path(args.output_csv))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

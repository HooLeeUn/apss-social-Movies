"""Deterministic, read-only planning for historical Movie duplicates."""
from collections import Counter
import hashlib
from itertools import groupby, islice
from pathlib import Path

from django.apps import apps
from django.db.models import Count

from core.management.commands.diagnose_local_movie_duplicates import (
    BATCH_SIZE, duplicate_groups, duplicate_records, json_text,
)
from core.models import Movie
from core.movie_duplicate_diagnosis import METADATA, WEIGHTS, technical_dependencies, usable


POLICY = "weighted-usable-metadata-v1"


def choose_survivor(records):
    scores = {}
    for row in records:
        fields = [name for name in METADATA if name in row and usable(name, row[name])
                  and (name != "genre" or any(part.strip() for part in row[name].split(",")))]
        scores[row["id"]] = {"score": sum(WEIGHTS[name] for name in fields), "usable_fields": fields}
    survivor = min(scores, key=lambda pk: (-scores[pk]["score"], pk))
    discarded = sorted(pk for pk in scores if pk != survivor)
    return {
        "keep_id": survivor,
        "remove_ids": discarded,
        "scores": scores,
        "reason": "highest weighted usable metadata coverage; lowest Movie.id on a tie",
        "metadata_sha256": hashlib.sha256(json_text(sorted(records, key=lambda row: row["id"])).encode()).hexdigest(),
    }


def dependency_paths(edges):
    def walk(parent, prefix, ancestors):
        for edge in edges:
            if edge["parent"] != parent:
                continue
            child = edge["dependent"]
            path = (edge["field"], *prefix)
            yield edge, "__".join(path)
            if edge["on_delete"] == "CASCADE" and child not in ancestors:
                yield from walk(child, path, (*ancestors, child))

    return list(walk(Movie._meta.label, (), (Movie._meta.label,)))


def dependency_counts(alias, paths, ids):
    result = {}
    for edge, path in paths:
        dependent = apps.get_model(edge["dependent"])
        rows = (dependent._base_manager.using(alias).filter(**{path + "__in": ids})
                .order_by().values(path).annotate(row_count=Count("pk")))
        for row in rows:
            result.setdefault(row[path], []).append({
                "model": edge["dependent"], "path": path, "on_delete": edge["on_delete"],
                "rows": row["row_count"],
            })
    return result


def read_schema(cursor, edges):
    tables = sorted({Movie._meta.db_table, *(edge["table"] for edge in edges)})
    qualified = ["public." + table for table in tables]
    cursor.execute("""
        SELECT c.conrelid::regclass::text, c.confrelid::regclass::text,
               child.attname, parent.attname, cardinality(c.conkey),
               c.confdeltype, c.convalidated, pg_get_constraintdef(c.oid)
        FROM pg_catalog."pg_constraint" c
        LEFT JOIN pg_attribute child ON child.attrelid=c.conrelid AND child.attnum=c.conkey[1]
        LEFT JOIN pg_attribute parent ON parent.attrelid=c.confrelid AND parent.attnum=c.confkey[1]
        WHERE c.contype='f' AND c.confrelid IN
            (SELECT to_regclass(name) FROM unnest(%s::text[]) AS name)
        ORDER BY 1, 2, 3, 8
    """, [qualified])
    fk_rows = cursor.fetchall()
    expected = set()
    for edge in edges:
        if edge["db_constraint"]:
            field = apps.get_model(edge["dependent"])._meta.get_field(edge["field"])
            expected.add((edge["table"], field.related_model._meta.db_table, field.column, field.target_field.column))
    observed = {tuple(row[:4]) for row in fk_rows if row[4] == 1 and row[5] in ("a", "r") and row[6]}
    blockers = []
    if expected != observed or len(observed) != len(fk_rows):
        blockers.append("Live incoming FK graph or delete actions differ from the reviewed Django graph.")
    cursor.execute("""
        SELECT conname, pg_get_constraintdef(oid)
        FROM pg_catalog."pg_constraint" WHERE conrelid='public.core_movie'::regclass ORDER BY conname
    """)
    constraints = cursor.fetchall()
    cursor.execute("""
        SELECT pg_get_indexdef(indexrelid) FROM pg_catalog."pg_index"
        WHERE indrelid='public.core_movie'::regclass ORDER BY 1
    """)
    indexes = [row[0] for row in cursor.fetchall()]
    cursor.execute("""
        SELECT tgrelid::regclass::text, tgname, tgenabled, pg_get_triggerdef(oid)
        FROM pg_catalog."pg_trigger" WHERE NOT tgisinternal AND tgrelid IN
            (SELECT to_regclass(name) FROM unnest(%s::text[]) AS name)
        ORDER BY 1, 2
    """, [qualified])
    triggers = cursor.fetchall()
    if any(row[2] != "D" and ("DELETE" in row[3] or "TRUNCATE" in row[3]) for row in triggers):
        blockers.append("Review live DELETE/TRUNCATE triggers on candidate tables.")
    return {"incoming_foreign_keys": fk_rows, "movie_constraints": constraints,
            "movie_indexes": indexes, "user_triggers": triggers, "blockers": blockers,
            "fk_graph_matches": expected == observed and len(observed) == len(fk_rows)}


def write_plan(alias, output, paths, *, estimate_dependencies):
    summary = Counter(groups=0, remove_movies=0, groups_with_dependencies=0, protected_groups=0)
    impact = Counter()
    path = Path(output) / "cleanup_plan.jsonl"
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        groups = iter(duplicate_groups(alias))
        while batch := list(islice(groups, BATCH_SIZE)):
            rows = duplicate_records(alias, batch)
            plans = []
            for (kind, tmdb_id), members in groupby(rows, key=lambda row: (row["type"], row["tmdb_id"])):
                plans.append({"type": kind, "tmdb_id": tmdb_id, **choose_survivor(list(members))})
            counts = dependency_counts(alias, paths, [pk for plan in plans for pk in plan["remove_ids"]]) \
                if estimate_dependencies else {}
            for plan in plans:
                dependencies = [dict(item, movie_id=pk) for pk in plan["remove_ids"] for item in counts.get(pk, ())]
                blocked = any(item["on_delete"] in ("PROTECT", "RESTRICT", "DO_NOTHING")
                              for item in dependencies)
                status = ("dependency_audit_incomplete" if not estimate_dependencies else
                          "blocked_by_relation" if blocked else "planned")
                plan.update(dependencies=dependencies, status=status)
                summary.update(groups=1, remove_movies=len(plan["remove_ids"]),
                               groups_with_dependencies=bool(dependencies), protected_groups=blocked)
                for item in dependencies:
                    impact[(item["model"], item["path"], item["on_delete"])] += item["rows"]
                stream.write(json_text(plan) + "\n")
    return {**summary, "impact_by_fk_path": [
        {"model": model, "path": path, "on_delete": behavior, "rows": count}
        for (model, path, behavior), count in sorted(impact.items())
    ]}


def django_dependencies():
    return technical_dependencies()["incoming_relations"]

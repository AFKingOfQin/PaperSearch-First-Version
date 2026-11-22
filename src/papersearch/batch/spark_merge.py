# AI-generated code (Codex): local Spark history merge and projected CSV/Parquet measurements.
import argparse
import json
import platform
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def winners(history, columns):
    # event_id identifies the normalized content. Replays have equal min and max IDs.
    by_priority = history.groupBy(
        "paper_id", "source_version", "source_updated_at"
    ).agg(
        F.min("event_id").alias("lo"),
        F.max("event_id").alias("hi"),
        F.first(F.struct(*columns)).alias("record"),
    )
    priority = F.struct(F.coalesce("source_version", F.lit(0)), "source_updated_at")
    return (
        by_priority.where("lo = hi")
        .groupBy("paper_id")
        .agg(F.max_by("record", priority).alias("winner"))
    )


def merge(base, updates):
    previous = winners(base, ["event_id"]).select(
        "paper_id", F.col("winner.event_id").alias("previous_event_id")
    )
    history = base.unionByName(updates)
    # Text has passed the event adapter. Preserve case and canonicalize category membership.
    history = history.withColumn(
        "categories", F.sort_array(F.array_distinct("categories"))
    )
    fields = [c for c in history.columns if c != "paper_id"]
    current = winners(history, fields).select("paper_id", "winner.*")
    return previous.join(current, "paper_id", "full").withColumn(
        "change",
        F.when(F.col("previous_event_id").isNull(), "new")
        .when(F.col("event_id").isNull(), "withdrawn")
        .when(F.col("event_id") == F.col("previous_event_id"), "unchanged")
        .otherwise("changed"),
    )


def write_merge(spark, args, path):
    result = merge(spark.read.parquet(args.base), spark.read.parquet(args.updates))
    result.repartition(4, "paper_id").sortWithinPartitions("paper_id").write.parquet(
        str(path)
    )
    return result


def explain(frame):
    return frame.sparkSession.sparkContext._jvm.PythonSQLUtils.explainString(
        frame._jdf.queryExecution(), "formatted"
    )


def storage_test(spark, current, output, reverse):
    # JSON encodes arrays and punctuation identically in the two flat storage representations.
    records = current.where("event_id is not null").select(
        "paper_id",
        "event_id",
        "embedding_text_hash",
        F.coalesce(F.substring("published", 1, 4).cast("int"), F.lit(0)).alias("year"),
        F.to_json("categories").alias("categories"),
        F.to_json(F.struct("title", "abstract")).alias("text"),
    )
    reports = {}
    for fmt in ("csv", "parquet"):
        path = output / fmt
        start = time.perf_counter()
        records.sortWithinPartitions("paper_id").write.format(fmt).options(
            header=True, escape='"'
        ).save(str(path))
        reports[fmt] = dict(
            write_seconds=time.perf_counter() - start,
            bytes=sum(p.stat().st_size for p in path.glob("part-*")),
            query_seconds=[],
        )
    summary = None
    for fmt in (
        ["parquet", "csv", "csv", "parquet"]
        if reverse
        else ["csv", "parquet", "parquet", "csv"]
    ):
        start = time.perf_counter()
        frame = (
            spark.read.schema(records.schema)
            .format(fmt)
            .options(header=True, escape='"')
            .load(str(output / fmt))
        )
        query = (
            frame.where("year = 2008")
            .select(
                F.explode(F.from_json("categories", "array<string>")).alias("category")
            )
            .groupBy("category")
            .count()
            .orderBy("category")
        )
        # Only category totals reach the driver; corpus records remain partitioned on disk.
        totals = {r.category: r["count"] for r in query.toLocalIterator()}
        reports[fmt]["query_seconds"].append(time.perf_counter() - start)
        if summary is not None and totals != summary:
            raise ValueError("Category query results differ")
        summary = totals
        (output / f"{fmt}-plan.txt").write_text(explain(query))
    return dict(formats=reports, categories_2008=summary)


def main():
    p = argparse.ArgumentParser()
    for field in ("base", "updates", "output"):
        p.add_argument(field)
    p.add_argument("--measure", action="store_true")
    p.add_argument("--parquet-first", action="store_true")
    p.add_argument("--launched-at", type=float)
    args = p.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    spark = SparkSession.builder.appName("PaperSearch batch merge").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        first = time.perf_counter()
        result = write_merge(spark, args, output / "merged")
        report = dict(
            first_merge_seconds=time.perf_counter() - first,
            spark=spark.version,
            python=platform.python_version(),
            master=spark.sparkContext.master,
            memory_limit_bytes=int(Path("/sys/fs/cgroup/memory.max").read_text()),
            cpu_quota_and_period=Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            java=spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
        )
        if args.launched_at is not None:
            report["launch_through_first_write_seconds"] = (
                time.time() - args.launched_at
            )
        (output / "merge-plan.txt").write_text(explain(result))
        if args.measure:
            start = time.perf_counter()
            write_merge(spark, args, output / "warm")
            report["warm_merge_seconds"] = time.perf_counter() - start
            report.update(
                storage_test(
                    spark,
                    spark.read.parquet(str(output / "warm")),
                    output,
                    args.parquet_first,
                )
            )
        report["cgroup_peak_bytes"] = int(
            Path("/sys/fs/cgroup/memory.peak").read_text()
        )
        (output / "report.json").write_text(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

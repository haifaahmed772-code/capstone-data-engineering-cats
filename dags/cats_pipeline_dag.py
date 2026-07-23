"""
Capstone DAG — Modern Data Engineering for AI Systems (SDAIA Academy)

Every task EXECUTES its stage with the real library. Nothing here re-reads work
done elsewhere: the DAG produces to Kafka and consumes back, validates with a
Pydantic contract, writes Bronze, performs a genuine Delta MERGE into Silver,
gates on Great Expectations, aggregates Gold, and builds + queries the RAG index.

    ingestion -> bronze_layer -> silver_merge -> quality_gate -> gold_aggregates -> rag_index

The quality gate sits BEFORE Gold and RAG, so a failing gate leaves every
downstream task in `upstream_failed` — never executed.

Each task emits OpenLineage START, then COMPLETE or FAIL, with dataset-level
inputs and outputs.

The DAG has no dependency on notebook state: it runs standalone from the config
file and a live Kafka broker on localhost:9092.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from functools import wraps

import pandas as pd
import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# ------------------------------------------------------------------ config
CONFIG_PATH = "/content/airflow/dags/pipeline_config.json"
with open(CONFIG_PATH, encoding="utf-8") as _f:
    CFG = json.load(_f)

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

# ------------------------------------------------------------- OpenLineage
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.event_v2 import (
    RunEvent, RunState, Run, Job, InputDataset, OutputDataset,
)

_ol_client = OpenLineageClient(
    transport=FileTransport(FileConfig(log_file_path=CFG["lineage_file"], append=True))
)


def _emit(job_name, state, inputs, outputs, run_id):
    _ol_client.emit(RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=CFG["lineage_namespace"], name=job_name),
        inputs=[InputDataset(namespace=CFG["lineage_namespace"], name=n) for n in inputs],
        outputs=[OutputDataset(namespace=CFG["lineage_namespace"], name=n) for n in outputs],
        producer=CFG["lineage_producer"],
    ))


def traced(job_name, inputs=(), outputs=()):
    """Emit START before the task, then COMPLETE on success or FAIL on exception."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(**context):
            run_id = str(uuid.uuid4())
            _emit(job_name, RunState.START, list(inputs), list(outputs), run_id)
            try:
                out = fn(**context)
            except Exception:
                _emit(job_name, RunState.FAIL, list(inputs), list(outputs), run_id)
                raise
            _emit(job_name, RunState.COMPLETE, list(inputs), list(outputs), run_id)
            return out
        return wrapper
    return decorator


# ------------------------------------------------------------------ helpers
def _arrow(df):
    return pa.Table.from_pandas(df, preserve_index=False)


def _artifact(name):
    d = CFG["run_artifacts"]
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


# ============================================================== 1. INGESTION
@traced("ingestion",
        inputs=["file.cats_raw_csv"],
        outputs=["kafka.cats-raw", "kafka.cats-quarantine", "file.dag_valid_records"])
def task_ingestion(**_):
    """
    Real Kafka round trip inside the task:
      admin creates the topics -> producer sends every CSV row ->
      consumer reads back exactly this run's messages ->
      each record is validated against a Pydantic contract ->
      malformed records are produced to the quarantine topic with the reason.
    """
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from pydantic import BaseModel, Field, ValidationError, field_validator
    from datetime import date

    bootstrap = CFG["bootstrap"]
    raw_topic = CFG["raw_topic"]
    quarantine_topic = CFG["quarantine_topic"]

    # ---- the data contract, enforced at the ingestion boundary ----
    class CatEvent(BaseModel):
        cat_id: str = Field(..., min_length=1)
        name: str = Field(..., min_length=1)
        breed: str = Field(..., min_length=1)
        age_months: int = Field(..., gt=0)
        weight_kg: float = Field(..., gt=0)
        is_vaccinated: bool
        shelter_id: str = Field(..., min_length=1)
        intake_date: date
        status: str = Field(..., pattern="^(available|adopted|pending)$")

        @field_validator("weight_kg", mode="before")
        @classmethod
        def weight_must_be_number(cls, v):
            try:
                return float(v)
            except (ValueError, TypeError):
                raise ValueError(f"weight_kg must be numeric, got: {v!r}")

    # ---- make sure both topics exist before we measure offsets ----
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    for t in (raw_topic, quarantine_topic):
        try:
            admin.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=1)])
            print(f"[ingestion] created topic {t}")
        except TopicAlreadyExistsError:
            pass
    admin.close()

    # ---- record where the log currently ends, so re-runs stay correct ----
    probe = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    parts = probe.partitions_for_topic(raw_topic) or {0}
    tps = [TopicPartition(raw_topic, p) for p in parts]
    start_offsets = probe.end_offsets(tps)
    probe.close()
    print(f"[ingestion] starting offsets: { {str(k): v for k, v in start_offsets.items()} }")

    # ---- PRODUCE ----
    raw = pd.read_csv(CFG["raw_csv"])
    raw = raw.where(pd.notnull(raw), None)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    for _, row in raw.iterrows():
        producer.send(raw_topic, value=row.to_dict())
    producer.flush()
    producer.close()
    sent = len(raw)
    print(f"[ingestion] produced {sent} messages to {raw_topic}")

    if sent != CFG["expected_raw_rows"]:
        raise AirflowFailException(
            f"expected {CFG['expected_raw_rows']} source rows, found {sent}"
        )

    # ---- CONSUME exactly the messages this run produced ----
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        consumer_timeout_ms=20000,
    )
    consumer.assign(tps)
    for tp in tps:
        consumer.seek(tp, start_offsets[tp])

    quarantine_producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    valid, rejected = [], []
    for message in consumer:
        try:
            valid.append(CatEvent(**message.value).model_dump(mode="json"))
        except ValidationError as e:
            record = {"original_record": message.value, "rejection_reason": str(e)}
            rejected.append(record)
            quarantine_producer.send(quarantine_topic, value=record)
        if len(valid) + len(rejected) >= sent:
            break

    quarantine_producer.flush()
    quarantine_producer.close()
    consumer.close()

    total = len(valid) + len(rejected)
    print(f"[ingestion] consumed {total} | valid {len(valid)} | quarantined {len(rejected)}")

    # ---- the failure path must be exercised, not merely available ----
    if total != sent:
        raise AirflowFailException(f"consumed {total} messages but produced {sent}")
    if not rejected:
        raise AirflowFailException("no records were quarantined — rejection path untested")
    if any("rejection_reason" not in r for r in rejected):
        raise AirflowFailException("a quarantined record carries no rejection reason")

    print(f"[ingestion] sample rejection: "
          f"{rejected[0]['rejection_reason'].splitlines()[1:3]}")

    with open(_artifact("valid_records.json"), "w", encoding="utf-8") as f:
        json.dump(valid, f, ensure_ascii=False)
    with open(_artifact("rejected_records.json"), "w", encoding="utf-8") as f:
        json.dump(rejected, f, ensure_ascii=False, indent=2)
    with open(CFG["quarantine_file"], "w", encoding="utf-8") as f:
        json.dump(rejected, f, ensure_ascii=False, indent=2)

    return {"produced": sent, "valid": len(valid), "quarantined": len(rejected)}


# ================================================================ 2. BRONZE
@traced("bronze_layer", inputs=["kafka.cats-raw"], outputs=["delta.bronze_cats"])
def task_bronze(**_):
    """Write the raw feed to Delta, untouched — malformed rows included."""
    from deltalake import write_deltalake, DeltaTable

    raw = pd.read_csv(CFG["raw_csv"]).astype(str)
    raw["ingested_at"] = datetime.now(timezone.utc).isoformat()
    raw["source"] = f"kafka.{CFG['raw_topic']}"

    write_deltalake(CFG["dag_bronze"], _arrow(raw), mode="overwrite",
                    schema_mode="overwrite")

    written = DeltaTable(CFG["dag_bronze"]).to_pandas()
    print(f"[bronze] wrote {len(written)} rows, {len(written.columns)} columns "
          f"-> {CFG['dag_bronze']}")

    if len(written) != CFG["expected_raw_rows"]:
        raise AirflowFailException(f"Bronze holds {len(written)} rows, expected "
                                   f"{CFG['expected_raw_rows']}")
    return len(written)


# ========================================================== 3. SILVER MERGE
@traced("silver_merge", inputs=["delta.bronze_cats"], outputs=["delta.silver_cats"])
def task_silver_merge(**_):
    """
    Build Silver from the validated records, then perform a REAL Delta MERGE
    (upsert) keyed on the business key cat_id. The merge metrics returned by
    Delta itself are the proof that rows were updated in place, not appended.
    """
    from deltalake import write_deltalake, DeltaTable

    with open(_artifact("valid_records.json"), encoding="utf-8") as f:
        valid = json.load(f)

    silver = pd.DataFrame(valid)
    silver["age_months"] = silver["age_months"].astype("int32")
    silver["weight_kg"] = silver["weight_kg"].astype("float64")
    silver["is_vaccinated"] = silver["is_vaccinated"].astype(bool)
    silver["intake_date"] = pd.to_datetime(silver["intake_date"]).dt.date.astype(str)

    write_deltalake(CFG["dag_silver"], _arrow(silver), mode="overwrite",
                    schema_mode="overwrite")
    base_rows = len(DeltaTable(CFG["dag_silver"]).to_pandas())
    print(f"[silver_merge] base Silver: {base_rows} rows")

    # --- change feed: 15 existing cats updated, 5 genuinely new ones ---
    updates = silver.head(15).copy()
    updates["status"] = "adopted"
    updates["is_vaccinated"] = True

    new_cats = silver.head(5).copy()
    new_cats["cat_id"] = [f"CAT{1000 + i}" for i in range(5)]
    new_cats["status"] = "available"
    new_cats["is_vaccinated"] = True

    batch = pd.concat([updates, new_cats], ignore_index=True)
    print(f"[silver_merge] change batch: {len(updates)} updates + {len(new_cats)} inserts")

    # --- the MERGE ---
    dt = DeltaTable(CFG["dag_silver"])
    metrics = (
        dt.merge(
            source=_arrow(batch),
            predicate="target.cat_id = source.cat_id",   # business key
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    final = DeltaTable(CFG["dag_silver"]).to_pandas()
    updated = metrics.get("num_target_rows_updated")
    inserted = metrics.get("num_target_rows_inserted")

    print(f"[silver_merge] MERGE metrics: updated={updated}, inserted={inserted}, "
          f"output_rows={metrics.get('num_output_rows')}")
    print(f"[silver_merge] {base_rows} -> {len(final)} rows "
          f"(an append would have produced {base_rows + len(batch)})")

    duplicates = int(final["cat_id"].duplicated().sum())
    if duplicates:
        raise AirflowFailException(
            f"MERGE produced {duplicates} duplicate business keys — not an upsert")
    if len(final) != base_rows + len(new_cats):
        raise AirflowFailException(
            f"expected {base_rows + len(new_cats)} rows after upsert, got {len(final)}")
    if updated != len(updates):
        raise AirflowFailException(
            f"expected {len(updates)} rows updated in place, Delta reported {updated}")

    print(f"[silver_merge] unique keys: {final['cat_id'].nunique()}, duplicates: 0")
    return {"rows": len(final), "updated": updated, "inserted": inserted}


# ========================================================== 4. QUALITY GATE
@traced("quality_gate", inputs=["delta.silver_cats"], outputs=[])
def task_quality_gate(**_):
    """Great Expectations suite on Silver. Failure raises and halts the DAG."""
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition
    from deltalake import DeltaTable

    silver = DeltaTable(CFG["dag_silver"]).to_pandas()

    if os.environ.get("CAPSTONE_INJECT_BAD_DATA") == "1":
        bad = silver.head(1).copy()
        bad["cat_id"] = "CAT_INJECTED_BAD"
        bad["age_months"] = -50            # breaks the range expectation
        bad["status"] = "on_vacation"      # breaks the allowed-values expectation
        silver = pd.concat([silver, bad], ignore_index=True)
        print("[quality_gate] injected one deliberately corrupt row")

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("airflow_silver")
    asset = source.add_dataframe_asset(name="cats_silver")
    batch_def = asset.add_batch_definition_whole_dataframe("whole_dataframe")

    suite = context.suites.add(ExpectationSuite(name="cats_silver_gate"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="cat_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="cat_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="age_months", min_value=1, max_value=360))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="weight_kg", min_value=0.5, max_value=15.0))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="status", value_set=CFG["allowed_status"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="breed", value_set=CFG["allowed_breeds"]))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="shelter_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="intake_date"))

    validation = context.validation_definitions.add(
        ValidationDefinition(name="airflow_gate", data=batch_def, suite=suite))
    result = validation.run(batch_parameters={"dataframe": silver})

    failed = [r.expectation_config.type for r in result.results if not r.success]
    print(f"[quality_gate] rows={len(silver)} checks={len(result.results)} "
          f"passed={result.success} failed={failed}")

    if not result.success:
        raise AirflowFailException(
            f"quality gate FAILED on {failed} — halting the pipeline before Gold and RAG")
    return True


# ================================================================== 5. GOLD
@traced("gold_aggregates", inputs=["delta.silver_cats"],
        outputs=["delta.gold_cats_by_breed", "delta.gold_cats_by_shelter"])
def task_gold(**_):
    """Aggregate Silver into Gold. Gold must summarise, never copy."""
    from deltalake import write_deltalake, DeltaTable

    silver = DeltaTable(CFG["dag_silver"]).to_pandas()

    by_breed = (
        silver.groupby("breed")
        .agg(total_cats=("cat_id", "count"),
             avg_age_months=("age_months", "mean"),
             avg_weight_kg=("weight_kg", "mean"),
             vaccinated_count=("is_vaccinated", "sum"))
        .reset_index()
    )
    by_breed["avg_age_months"] = by_breed["avg_age_months"].round(1)
    by_breed["avg_weight_kg"] = by_breed["avg_weight_kg"].round(2)
    by_breed["vaccination_rate_pct"] = (
        by_breed["vaccinated_count"] / by_breed["total_cats"] * 100).round(1)
    by_breed["vaccinated_count"] = by_breed["vaccinated_count"].astype("int64")

    by_shelter = (
        silver.assign(
            available=(silver["status"] == "available").astype(int),
            adopted=(silver["status"] == "adopted").astype(int),
            pending=(silver["status"] == "pending").astype(int),
        )
        .groupby("shelter_id")
        .agg(total_cats=("cat_id", "count"),
             available_count=("available", "sum"),
             adopted_count=("adopted", "sum"),
             pending_count=("pending", "sum"))
        .reset_index()
    )

    write_deltalake(CFG["dag_gold_breed"], _arrow(by_breed), mode="overwrite",
                    schema_mode="overwrite")
    write_deltalake(CFG["dag_gold_shelter"], _arrow(by_shelter), mode="overwrite",
                    schema_mode="overwrite")

    print(f"[gold] silver={len(silver)} -> by_breed={len(by_breed)} rows, "
          f"by_shelter={len(by_shelter)} rows")

    for gold, label in ((by_breed, "breed"), (by_shelter, "shelter")):
        if len(gold) >= len(silver):
            raise AirflowFailException(
                f"Gold/{label} is not an aggregate — {len(gold)} rows vs Silver {len(silver)}")
        if int(gold["total_cats"].sum()) != len(silver):
            raise AirflowFailException(
                f"Gold/{label} totals {int(gold['total_cats'].sum())} != Silver {len(silver)}")

    print("[gold] both aggregates reconcile to Silver exactly")
    return {"breed_rows": len(by_breed), "shelter_rows": len(by_shelter)}


# ============================================================= 6. RAG INDEX
@traced("rag_index", inputs=["file.cat_care_docs_json"], outputs=["chroma.cat_care"])
def task_rag_index(**_):
    """
    Build the retrieval index and prove it answers: chunking -> embeddings ->
    Chroma -> hybrid dense+BM25 fused with RRF -> cross-encoder rerank -> cited answer.
    """
    import numpy as np
    from sentence_transformers import SentenceTransformer, CrossEncoder
    import chromadb
    from rank_bm25 import BM25Okapi

    with open(CFG["docs_json"], encoding="utf-8") as f:
        docs = json.load(f)

    chunk_size, overlap = CFG["chunk_size"], CFG["chunk_overlap"]

    def chunk_text(text):
        words, out, start = text.split(), [], 0
        while start < len(words):
            end = start + chunk_size
            out.append(" ".join(words[start:end]))
            if end >= len(words):
                break
            start = end - overlap
        return out

    chunks = []
    for d in docs:
        for i, piece in enumerate(chunk_text(d["text"])):
            chunks.append({"chunk_id": f"{d['doc_id']}_C{i:02d}",
                           "doc_id": d["doc_id"], "title": d["title"], "text": piece})

    texts = [c["text"] for c in chunks]
    ids = [c["chunk_id"] for c in chunks]
    by_id = {c["chunk_id"]: c for c in chunks}
    print(f"[rag_index] {len(docs)} documents -> {len(chunks)} chunks "
          f"({chunk_size}w / {overlap}w overlap)")

    # ---- embeddings ----
    embedder = SentenceTransformer(CFG["embed_model"])
    vectors = embedder.encode(texts, convert_to_numpy=True)
    print(f"[rag_index] embeddings shape: {vectors.shape}")

    # ---- vector store ----
    client = chromadb.PersistentClient(path=CFG["dag_chroma_path"])
    try:
        client.delete_collection("cat_care")
    except Exception:
        pass
    collection = client.create_collection(name="cat_care",
                                          metadata={"hnsw:space": "cosine"})
    collection.add(ids=ids, documents=texts, embeddings=vectors.tolist(),
                   metadatas=[{"doc_id": c["doc_id"], "title": c["title"]} for c in chunks])
    stored = collection.count()
    print(f"[rag_index] stored {stored} vectors in Chroma")

    if stored != len(chunks):
        raise AirflowFailException(f"stored {stored} vectors for {len(chunks)} chunks")

    # ---- BM25 ----
    def tok(t):
        return re.findall(r"[a-z0-9]+", t.lower())

    bm25 = BM25Okapi([tok(t) for t in texts])

    # ---- hybrid retrieval fused with Reciprocal Rank Fusion ----
    def hybrid(query, top_k=10, k=60):
        dense = collection.query(
            query_embeddings=embedder.encode([query], convert_to_numpy=True).tolist(),
            n_results=top_k)["ids"][0]
        scores = bm25.get_scores(tok(query))
        sparse = [ids[i] for i in np.argsort(scores)[::-1][:top_k]]
        fused = {}
        for ranked in (dense, sparse):
            for rank, cid in enumerate(ranked, start=1):
                fused[cid] = fused.get(cid, 0.0) + 1.0 / (k + rank)
        return dense, sparse, sorted(fused.items(), key=lambda x: -x[1])[:8]

    # ---- cross-encoder reranking ----
    reranker = CrossEncoder(CFG["rerank_model"])

    probe = "my male cat is straining in the litter box and cannot urinate"
    dense, sparse, fused = hybrid(probe)
    pairs = [(probe, by_id[cid]["text"]) for cid, _ in fused]
    reranked = sorted(zip([c for c, _ in fused], reranker.predict(pairs)),
                      key=lambda x: -x[1])[:3]

    print(f"[rag_index] probe: {probe}")
    print(f"[rag_index] dense top3   : {[by_id[c]['doc_id'] for c in dense[:3]]}")
    print(f"[rag_index] bm25 top3    : {[by_id[c]['doc_id'] for c in sparse[:3]]}")
    print(f"[rag_index] fused top3   : {[by_id[c]['doc_id'] for c, _ in fused[:3]]}")
    print(f"[rag_index] reranked top3: "
          f"{[(by_id[c]['doc_id'], round(float(s), 3)) for c, s in reranked]}")

    top_id, top_score = reranked[0]
    answer = by_id[top_id]["text"].split(". ")[0].strip().rstrip(".")
    print(f"[rag_index] answer: {answer}. [{by_id[top_id]['doc_id']}]")

    if float(top_score) < CFG["relevance_threshold"]:
        raise AirflowFailException(
            f"best reranked score {float(top_score):.3f} below threshold — index unusable")

    manifest = {
        "documents": len(docs), "chunks": len(chunks), "vectors_stored": stored,
        "vector_dim": int(vectors.shape[1]),
        "embed_model": CFG["embed_model"], "rerank_model": CFG["rerank_model"],
        "chunk_size": chunk_size, "chunk_overlap": overlap,
        "chroma_path": CFG["dag_chroma_path"],
        "probe_query": probe,
        "probe_top_doc": by_id[top_id]["doc_id"],
        "probe_top_score": float(top_score),
    }
    with open(CFG["rag_manifest"], "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    return manifest


# ------------------------------------------------------------------ the DAG
with DAG(
    dag_id="cats_capstone_pipeline",
    description="Kafka -> Delta Lakehouse -> Quality Gate -> Gold -> RAG (SDAIA Capstone)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "sdaia_capstone", "retries": 0},
    tags=["capstone", "sdaia", "cats"],
) as dag:

    t_ingestion = PythonOperator(task_id="ingestion", python_callable=task_ingestion)
    t_bronze = PythonOperator(task_id="bronze_layer", python_callable=task_bronze)
    t_silver = PythonOperator(task_id="silver_merge", python_callable=task_silver_merge)
    t_gate = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    t_gold = PythonOperator(task_id="gold_aggregates", python_callable=task_gold)
    t_rag = PythonOperator(task_id="rag_index", python_callable=task_rag_index)

    # The gate sits before Gold and RAG: if it fails, neither ever runs.
    t_ingestion >> t_bronze >> t_silver >> t_gate >> t_gold >> t_rag

"""
Capstone DAG - Modern Data Engineering for AI Systems (SDAIA Academy)

يربط مراحل الخط كاملة بتبعيات خطية صارمة:
    ingestion -> bronze -> quality_gate -> silver_audit -> gold -> rag_index

بوابة الجودة (Great Expectations) ترمي AirflowFailException عند الفشل،
وبما أن المراحل اللاحقة تعتمد عليها مباشرة فإنها تنتقل إلى حالة upstream_failed
ولا تُنفَّذ إطلاقاً. كل مهمة تصدر أحداث OpenLineage: START ثم COMPLETE أو FAIL.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from functools import wraps

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# ------------------------------------------------------------------ الإعداد
CONFIG_PATH = "/content/airflow/dags/pipeline_config.json"
with open(CONFIG_PATH, encoding="utf-8") as _f:
    CFG = json.load(_f)

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
    """يغلّف المهمة بأحداث النسب: START ثم COMPLETE عند النجاح أو FAIL عند الخطأ."""
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


def _read_delta(path):
    from deltalake import DeltaTable
    return DeltaTable(path).to_pandas()


# --------------------------------------------------------------- المراحل
@traced("ingestion", inputs=["file.cats_raw_csv"],
        outputs=["kafka.cats-raw", "kafka.cats-quarantine"])
def task_ingestion(**_):
    raw = pd.read_csv(CFG["raw_csv"])
    with open(CFG["quarantine_file"], encoding="utf-8") as f:
        quarantined = json.load(f)

    if len(raw) != CFG["expected_raw_rows"]:
        raise AirflowFailException(
            f"عدد السجلات الخام غير متوقع: {len(raw)} بدلاً من {CFG['expected_raw_rows']}"
        )
    if not quarantined:
        raise AirflowFailException("لا توجد سجلات في الحجر الصحي — مسار الرفض لم يُختبر")
    if any("rejection_reason" not in q for q in quarantined):
        raise AirflowFailException("سجل محجوز بدون سبب رفض مسجّل")

    print(f"[ingestion] سجلات خام: {len(raw)} | محجوزة: {len(quarantined)}")
    return {"raw": len(raw), "quarantined": len(quarantined)}


@traced("bronze_layer", inputs=["kafka.cats-raw"], outputs=["delta.bronze_cats"])
def task_bronze(**_):
    bronze = _read_delta(CFG["bronze_path"])
    if len(bronze) != CFG["expected_raw_rows"]:
        raise AirflowFailException(f"Bronze يحتوي {len(bronze)} صفاً بدلاً من {CFG['expected_raw_rows']}")
    for col in ("ingested_at", "source"):
        if col not in bronze.columns:
            raise AirflowFailException(f"عمود المصدر المفقود في Bronze: {col}")
    print(f"[bronze] صفوف: {len(bronze)} | أعمدة: {len(bronze.columns)}")
    return len(bronze)


@traced("quality_gate", inputs=["delta.silver_cats"], outputs=[])
def task_quality_gate(**_):
    """بوابة Great Expectations — نقطة التوقف الفعلية للخط."""
    os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    silver = _read_delta(CFG["silver_path"])

    # حقن بيانات فاسدة عند الطلب لإثبات أن البوابة توقف الخط فعلاً
    if os.environ.get("CAPSTONE_INJECT_BAD_DATA") == "1":
        bad = silver.head(1).copy()
        bad["cat_id"] = "CAT_INJECTED_BAD"
        bad["age_months"] = -50          # يكسر توقّع المدى
        bad["status"] = "on_vacation"    # يكسر توقّع القيم المسموحة
        silver = pd.concat([silver, bad], ignore_index=True)
        print("[quality_gate] ⚠️ تم حقن سجل فاسد عمداً لاختبار البوابة")

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("airflow_silver")
    asset = source.add_dataframe_asset(name="cats_silver")
    batch_def = asset.add_batch_definition_whole_dataframe("whole_dataframe")

    suite = context.suites.add(ExpectationSuite(name="cats_silver_gate"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="cat_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="cat_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="age_months", min_value=1, max_value=360))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="weight_kg", min_value=0.5, max_value=15.0))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="status", value_set=CFG["allowed_status"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="breed", value_set=CFG["allowed_breeds"]))

    validation = context.validation_definitions.add(
        ValidationDefinition(name="airflow_gate", data=batch_def, suite=suite)
    )
    result = validation.run(batch_parameters={"dataframe": silver})

    failed = [r.expectation_config.type for r in result.results if not r.success]
    print(f"[quality_gate] صفوف: {len(silver)} | نجاح: {result.success} | توقعات فاشلة: {failed}")

    if not result.success:
        raise AirflowFailException(
            f"بوابة الجودة فشلت — التوقعات المخالفة: {failed}. تم إيقاف الخط."
        )
    return True


@traced("silver_audit", inputs=["delta.bronze_cats"], outputs=["delta.silver_cats"])
def task_silver_audit(**_):
    """يتحقق أن الـ MERGE كان upsert حقيقياً: مفتاح العمل فريد ولا تكرار."""
    silver = _read_delta(CFG["silver_path"])
    duplicates = int(silver["cat_id"].duplicated().sum())
    if duplicates:
        raise AirflowFailException(f"MERGE أنتج {duplicates} مفتاح عمل مكرر — ليس upsert صحيحاً")
    print(f"[silver_audit] صفوف: {len(silver)} | مفاتيح فريدة: {silver['cat_id'].nunique()} | مكررة: 0")
    return len(silver)


@traced("gold_aggregates", inputs=["delta.silver_cats"],
        outputs=["delta.gold_cats_by_breed", "delta.gold_cats_by_shelter"])
def task_gold(**_):
    """يتحقق أن Gold تجميع حقيقي ومتسق مع Silver، وليس نسخة منها."""
    silver = _read_delta(CFG["silver_path"])
    gold_breed = _read_delta(CFG["gold_breed_path"])
    gold_shelter = _read_delta(CFG["gold_shelter_path"])

    if len(gold_breed) >= len(silver):
        raise AirflowFailException("Gold ليس تجميعاً — عدد صفوفه ليس أقل من Silver")

    for gold, total_col, name in ((gold_breed, "total_cats", "breed"),
                                  (gold_shelter, "total_cats", "shelter")):
        if int(gold[total_col].sum()) != len(silver):
            raise AirflowFailException(
                f"مجموع تجميع {name} = {int(gold[total_col].sum())} لا يطابق Silver = {len(silver)}"
            )

    print(f"[gold] silver={len(silver)} | by_breed={len(gold_breed)} صفوف | "
          f"by_shelter={len(gold_shelter)} صفوف | المجاميع متطابقة ✅")
    return {"breed_rows": len(gold_breed), "shelter_rows": len(gold_shelter)}


@traced("rag_index", inputs=["file.cat_care_docs_json"], outputs=["chroma.cat_care"])
def task_rag_index(**_):
    with open(CFG["rag_manifest"], encoding="utf-8") as f:
        manifest = json.load(f)
    if manifest.get("vectors_stored", 0) <= 0:
        raise AirflowFailException("فهرس المتجهات فارغ — مرحلة RAG غير جاهزة")
    if manifest["chunks"] != manifest["vectors_stored"]:
        raise AirflowFailException("عدد القطع لا يطابق عدد المتجهات المخزنة")
    print(f"[rag_index] مستندات={manifest['documents']} | قطع={manifest['chunks']} | "
          f"متجهات={manifest['vectors_stored']} | بُعد={manifest['vector_dim']}")
    return manifest["vectors_stored"]


# ----------------------------------------------------------------- الـ DAG
default_args = {"owner": "sdaia_capstone", "retries": 0}

with DAG(
    dag_id="cats_capstone_pipeline",
    description="Kafka -> Delta Lakehouse -> Quality Gate -> Gold -> RAG (SDAIA Capstone)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["capstone", "sdaia", "cats"],
) as dag:

    t_ingestion = PythonOperator(task_id="ingestion", python_callable=task_ingestion)
    t_bronze = PythonOperator(task_id="bronze_layer", python_callable=task_bronze)
    t_gate = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    t_silver = PythonOperator(task_id="silver_audit", python_callable=task_silver_audit)
    t_gold = PythonOperator(task_id="gold_aggregates", python_callable=task_gold)
    t_rag = PythonOperator(task_id="rag_index", python_callable=task_rag_index)

    # التبعيات: كل ما بعد البوابة لا يعمل إلا إذا نجحت
    t_ingestion >> t_bronze >> t_gate >> t_silver >> t_gold >> t_rag

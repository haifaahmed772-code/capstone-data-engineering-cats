# Capstone: Modern Data Engineering for AI Systems

مشروع تخرج (Capstone) لبرنامج **SDAIA Academy** — Modern Data Engineering for AI Systems.

## فكرة المشروع

خط بيانات متكامل (end-to-end) لنظام ملجأ قطط، يجمع بين معالجة البيانات المهيكلة
ونظام استرجاع معرفي، ويُشغَّل بالكامل عبر منظّم واحد مع بوابة جودة وتتبع نسب البيانات.

المشكلة التي يعالجها: ملجأ الحيوانات يحتاج شيئين مختلفين في آن واحد — تقارير موثوقة
عن أعداد القطط وحالاتها (بيانات مهيكلة)، ومساعد يجيب أسئلة المتبنّين عن رعاية القطط
من مصادر موثوقة (نصوص غير مهيكلة). المشروع يبني الاثنين على بنية تحتية واحدة.

## المراحل

| المرحلة | الأدوات | الوصف |
|---|---|---|
| Ingestion | kafka-python, pydantic | Producer/Consumer فعليان، عقد بيانات Pydantic عند حدود الاستيعاب، والسجلات الفاسدة تُوجَّه إلى topic حجر صحي مع سبب الرفض |
| Delta Lakehouse | pyspark, delta-spark | طبقات Bronze / Silver / Gold، مع MERGE (upsert) على مفتاح العمل cat_id، وفرض صارم للـ schema |
| RAG Pipeline | sentence-transformers, chromadb, rank-bm25 | تقطيع بتداخل، embeddings، مخزن متجهات Chroma، بحث هجين (Dense + BM25) مدموج بـ RRF، إعادة ترتيب بـ cross-encoder، وإجابات مرتكزة على السياق مع استشهادات |
| Orchestration | apache-airflow | DAG يربط كل المراحل بتبعيات صارمة، وبوابة الجودة توقف الخط قبل المراحل اللاحقة عند الفشل |
| Quality Gate + Lineage | great-expectations, openlineage-python | مجموعة توقعات تحكم مرور الخط، وأحداث START / COMPLETE / FAIL لكل مرحلة |

## النتائج الرئيسية

- 200 سجل خام، 170 اجتازت عقد البيانات، 30 وُجّهت إلى الحجر الصحي مع سبب الرفض.
- MERGE رفع Silver من 170 إلى 175 صفاً (وليس 190) — إثبات رقمي أن العملية upsert وليست append.
- رفضان موثّقان لـ schema enforcement: عمود إضافي غير معرّف، ونوع بيانات غير متوافق.
- تشغيلان فعليان للـ DAG: واحد ناجح بالكامل، وآخر تفشل فيه البوابة فتتحول المراحل اللاحقة إلى upstream_failed.

## المتطلبات

- Python 3.12 (بيئة Google Colab)
- Java 17 (لازم لـ Kafka و Spark)
- الحزم: kafka-python, pydantic, faker, pyspark==3.5.1, delta-spark==3.2.0,
  sentence-transformers, chromadb, rank-bm25, great-expectations==1.3.11,
  openlineage-python==1.51.0, apache-airflow==2.11.2, deltalake

## طريقة التشغيل

1. افتح notebooks/capstone_SDAIA.ipynb في Google Colab.
2. اربط Google Drive عند الطلب في الخلية الأولى.
3. شغّل الخلايا بالترتيب من الأعلى إلى الأسفل (Runtime > Run all).
4. خلية بيانات اعتماد GitHub تتوقف بانتظار إدخال يدوي (اسم المستخدم، التوكن، اسم المستودع).
5. قسم Airflow يجب أن يُشغَّل في النهاية لأن تثبيته يغيّر نسخ حزم في الجلسة.

## هيكلة المستودع

- notebooks/ : النوت بوك التنفيذي مع كل المخرجات المحفوظة
- data/      : البيانات الخام، سجلات الحجر الصحي، مستندات قاعدة المعرفة، بيان فهرس المتجهات
- docs/      : توثيق تقني إضافي
- logs/      : سجل Kafka وملفات أحداث OpenLineage

## متغيرات البيئة

- AIRFLOW_HOME=/content/airflow
- GX_ANALYTICS_ENABLED=false
- CAPSTONE_INJECT_BAD_DATA=1 (اختياري، لإجبار بوابة الجودة على الفشل بغرض الإثبات)

## قيود معروفة وملاحظات صريحة

-- دفعة التحديثات في الـ MERGE مشتقّة من سجلات Silver نفسها (15 تحديثاً و5 إدراجات)،
  لا من تغذية تغييرات مستمرة من مصدر خارجي. عملية الدمج نفسها حقيقية ومقاييسها
  صادرة من محرّك Delta: updated=15, inserted=5, output_rows=175.
- إجابة الـ RAG استخراجية: تُبنى من القطع المسترجَعة مع استشهاداتها بدل توليدها بنموذج لغوي.
  الاسترجاع والدمج وإعادة الترتيب والاستشهادات كلها حقيقية، والصياغة النهائية هي النص المسترجَع.
  نقطة التوسعة محددة في الكود: يكفي تمرير نفس السياق إلى أي LLM دون تغيير باقي الخط.
- الاستهلاك من Kafka يستخدم assign() مع seek_to_beginning() بدل consumer group، بسبب تعارض
  معروف في بروتوكول تنسيق المجموعات بين kafka-python و Kafka 3.7. الاستهلاك يبقى حقيقياً من الوسيط.
- البيانات مُولَّدة بـ Faker مع تخريب متعمّد بنسبة 15%. هذا اختيار مقصود: التحكم في نوع كل عيب
  (عمر سالب، وزن نصي، مفتاح فارغ، حالة غير مسموحة، حقل ناقص) يسمح بإثبات كل مسار رفض على حدة.
- مخزن المتجهات Chroma يعيش على /content وليس على Drive، فيُعاد بناؤه مع كل جلسة جديدة.

## البرنامج التدريبي

**المتدربة:** Haifa Ahmed — [github.com/haifaahmed772-code](https://github.com/haifaahmed772-code)

مُنجَز ضمن **Modern Data Engineering for AI Systems**، SDAIA Academy، عبر Learning Space
— كابستون 5 أيام. المدرب: Mohammed Albeladi.

تواريخ الدفعة / الجلسة: **[19-7-2026 / 23-7-2026]**

## مرجع

[SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)

---

# English Summary

An end-to-end data engineering pipeline for a cat shelter, combining structured data
processing with a knowledge-retrieval system, orchestrated by a single Airflow DAG
with a blocking quality gate and dataset-level lineage.

**Problem:** a shelter needs two different things at once — reliable reports on cat
counts and statuses (structured data), and an assistant that answers adopters'
cat-care questions from trusted sources (unstructured text). This project builds
both on one platform.

## Where the code lives

All pipeline code is in the **executed notebook** `notebooks/capstone_SDAIA.ipynb`,
with output captured for every cell. The only standalone `.py` file is the Airflow
DAG (`dags/cats_pipeline_dag.py`), which contains orchestration logic only.

> **Auditing this repo with a script?** Parse the `.ipynb` cell sources.
> Scanning `*.py` alone will find the DAG and miss Kafka, Pydantic, the embeddings,
> BM25 and the reranker. See **[EVIDENCE.md](EVIDENCE.md)** for a cell-by-cell map
> of every rubric requirement to the exact library call and its captured output.

## Prerequisites

- Python 3.12 (Google Colab environment)
- Java 17 (required by Kafka and Spark; preinstalled in Colab)
- A Google account (the notebook mounts Google Drive for persistent storage)

## Setup and how to run

1. Open `notebooks/capstone_SDAIA.ipynb` in Google Colab.
2. Run the first cell and approve the **Google Drive** mount prompt.
   All persistent output is written to `MyDrive/capstone_data_engineering/`.
3. Run the cells **in order, top to bottom** (`Runtime → Run all`).
   Dependencies are installed by the notebook itself:
   ```
   kafka-python  pydantic  faker
   pyspark==3.5.1  delta-spark==3.2.0
   sentence-transformers  chromadb  rank-bm25
   great-expectations==1.3.11  openlineage-python==1.51.0
   apache-airflow==2.11.2  deltalake
   ```
4. Two cells require interaction:
   - the Drive mount prompt (step 2)
   - the GitHub credentials cell, which waits for username / token / repo name
     via `getpass` (skip it if you only want to run the pipeline)
5. **Run the Airflow section last.** Installing Airflow with its constraint file
   downgrades several packages in the session, so it must come after every other
   stage has run.

Expected wall-clock time: roughly 20–30 minutes, dominated by the Kafka download,
the PySpark install, and the HuggingFace model downloads.

## Expected output

| Stage | What you should see |
|---|---|
| Ingestion | Kafka broker starts; 200 records produced and consumed; **170 valid, 30 quarantined** with a rejection reason each |
| Delta Lakehouse | Bronze **200** rows → Silver **170**, then MERGE → **175** (not 190, proving upsert); two bad writes refused by schema enforcement |
| RAG | 12 documents → **32 chunks** → embeddings of shape **(32, 384)** in Chroma; hybrid dense+BM25 fused by RRF; cross-encoder reranking; cited answers, and an out-of-domain question refused |
| Quality gate | 8 Great Expectations checks **pass** on Silver, and **fail** on a deliberately corrupted copy |
| Orchestration | Healthy DAG run: all six tasks `success`. Injected-bad-data run: `quality_gate` **`failed`**, and every downstream task **`upstream_failed`** — never executed |
| Lineage | 14 OpenLineage events from the notebook and 18 from the DAG runs, covering `START` / `COMPLETE` / `FAIL`, written to `logs/*.jsonl` |

## Environment variables

| Variable | Purpose |
|---|---|
| `AIRFLOW_HOME=/content/airflow` | Airflow metadata and DAG folder |
| `AIRFLOW__CORE__LOAD_EXAMPLES=False` | Hide Airflow's bundled example DAGs |
| `GX_ANALYTICS_ENABLED=false` | Disable Great Expectations telemetry |
| `CAPSTONE_INJECT_BAD_DATA=1` | Optional — forces the quality gate to fail, to demonstrate that it halts the pipeline |

## Repository layout

| Path | Contents |
|---|---|
| `notebooks/` | The executed notebook with all captured output |
| `dags/` | `cats_pipeline_dag.py` — the Airflow DAG |
| `data/` | Raw CSV, quarantined records with reasons, RAG document corpus, vector-index manifest |
| `docs/` | `architecture.md` — pipeline overview |
| `logs/` | Kafka broker log and OpenLineage event files (JSONL) |
| `EVIDENCE.md` | Rubric requirement → cell number → library call → captured output |

## Training program

Completed under **Modern Data Engineering for AI Systems**, SDAIA Academy
(delivered via Learning Space) — 5-day capstone. Trainer: Mohammed Albeladi.
Cohort / session dates: **19-07-2026 to 23-07-2026**.
Trainee: Haifa Ahmed.

Reference: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)

# Capstone: Modern Data Engineering for AI Systems

مشروع تخرج (Capstone) لبرنامج **SDAIA Academy** — Modern Data Engineering for AI Systems.

## نبذة عن المشروع
Pipeline كامل لمعالجة بيانات (قطط - بيانات تجريبية عبر Faker) يغطي:
- **Ingestion**: Kafka producer/consumer مع schema validation (Pydantic) وعزل السجلات الخربانة (Quarantine).
- **Delta Lakehouse**: Bronze/Silver/Gold layers (قيد التنفيذ).
- **RAG Pipeline**: بحث هجين + reranking (قيد التنفيذ).
- **Orchestration**: Airflow DAG (قيد التنفيذ).
- **Quality Gate + Lineage**: Great Expectations + OpenLineage (قيد التنفيذ).

## البرنامج التدريبي
SDAIA Academy — Modern Data Engineering for AI Systems
المدرب: Mohammed Albeladi

## المتطلبات (Prerequisites)
- Python 3.10+
- Google Colab (البيئة المستخدمة لتشغيل المشروع)
- المكتبات: kafka-python, pydantic, faker, pandas

## طريقة التشغيل
1. افتح notebooks/capstone_SDAIA.ipynb على Google Colab.
2. اربط Google Drive.
3. شغّل الخلايا بالترتيب من الأعلى للأسفل.

## هيكلة المشروع
- notebooks/  : Jupyter/Colab notebooks
- data/       : بيانات تجريبية (CSV)
- docs/       : توثيق تقني إضافي
- logs/       : سجلات تشغيل Kafka وغيرها

## مرجع
[SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)

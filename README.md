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

- الـ MERGE في التشغيل الحالي يحدّث 15 مفتاحاً موجوداً ويدرج 5 جديدة؛ دفعة التحديث مبنية يدوياً
  من أوائل سجلات Silver، لذلك مسار التحديث مُختبَر لكنه ليس ناتجاً عن تغذية مستمرة من Kafka.
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

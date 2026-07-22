# Pipeline Architecture

cats_raw.csv
  -> Kafka topic: cats-raw            (producer)
  -> Pydantic contract CatEvent       (consumer, validation boundary)
       |-- valid   -> Delta Bronze -> Delta Silver (MERGE on cat_id) -> Delta Gold
       |-- invalid -> Kafka topic: cats-quarantine (with rejection_reason)

cat_care_docs.json
  -> chunking (60 words / 20 overlap)
  -> embeddings (all-MiniLM-L6-v2)
  -> ChromaDB collection: cat_care
  -> hybrid retrieval: dense + BM25 fused with RRF (k=60)
  -> cross-encoder rerank (ms-marco-MiniLM-L-6-v2)
  -> grounded answer with citations

Airflow DAG: cats_capstone_pipeline
  ingestion -> bronze_layer -> quality_gate -> silver_audit -> gold_aggregates -> rag_index

Quality gate: Great Expectations suite on Silver.
On failure it raises AirflowFailException, so every downstream task becomes upstream_failed.

Lineage: OpenLineage RunEvents (START / COMPLETE / FAIL) written to JSONL via FileTransport.

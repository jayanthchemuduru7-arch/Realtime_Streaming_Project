# Interview Guide - Real-Time Streaming Pipeline

## Project Elevator Pitch (30 seconds)

"I built a real-time streaming data engineering pipeline using Microsoft Fabric.
Data flows from Eventstream through a Medallion Architecture - Bronze for raw storage,
Silver for data quality and enrichment, and Gold for aggregated insights.
I integrated a Fabric Data Agent so business users can query data in plain English,
and Data Activator for automated real-time alerts. The entire pipeline processes
bike-sharing station data to track availability across London neighbourhoods."

---

## Technical Questions & Answers

### Q1: What is Medallion Architecture?
**A:** A data design pattern with three layers:
- **Bronze**: Raw data, no transformations. Source of truth.
- **Silver**: Cleaned, validated, enriched data. Business logic applied.
- **Gold**: Aggregated, business-ready data for dashboards and reports.

### Q2: Why Spark Structured Streaming instead of batch?
**A:** Batch processes data in scheduled intervals (hourly/daily), creating delay.
Structured Streaming processes data as it arrives with sub-second latency.
It treats streaming as an unbounded table, using the same DataFrame API as batch.

### Q3: What is checkpointing and why is it important?
**A:** Checkpointing saves the progress of a streaming query to durable storage.
If the job crashes and restarts, it resumes from the last checkpoint.
This ensures exactly-once processing and fault tolerance.

### Q4: What is watermarking?
**A:** Watermarking tells Spark: "Data arriving more than X minutes late should be ignored."
It prevents the state from growing infinitely in windowed aggregations.
Example: .withWatermark("event_time", "2 minutes")

### Q5: Explain append vs complete output modes.
**A:**
- **Append**: Only new rows are written. Used when no aggregations (Bronze, Silver).
- **Complete**: Entire result table is rewritten. Used with aggregations (Gold).

### Q6: Why Delta Lake instead of Parquet?
**A:** Delta Lake adds:
- ACID transactions (no partial writes)
- Schema enforcement
- Time travel (query historical versions)
- Upserts (MERGE)
- Compaction (optimize small files)

### Q7: How would you handle schema evolution?
**A:** Delta Lake supports: .option("mergeSchema", "true")
New columns are automatically added. Missing columns get NULL values.

### Q8: How does Fabric Eventstream differ from Azure Event Hub?
**A:**
- Event Hub: Azure PaaS, manual setup, for external sources
- Eventstream: Fabric native, visual no-code, built-in destinations

### Q9: How would you scale this pipeline?
**A:**
- Increase Event Hub partitions for higher throughput
- Add Spark executors for parallel processing
- Optimize Delta with Z-ordering and compaction
- Separate lakehouses per medallion layer
- Use Spark Job Definitions for production

### Q10: What monitoring would you add in production?
**A:**
- Spark Structured Streaming UI (input rate, batch duration)
- Delta table metrics (file count, size)
- Data Activator alerts for pipeline failures
- Azure Monitor for Event Hub throughput

---

## Behavioral Questions

### "Tell me about a challenging problem you solved."
"When connecting Event Hub to Fabric, I encountered CBS Token authentication errors.
After debugging, I discovered Fabric notebooks block AMQP port 5671.
I solved it by switching to WebSocket transport (port 443) and alternatively
used Fabric Eventstream for native integration, demonstrating both approaches."

### "How do you ensure data quality?"
"In my Silver layer, I applied multiple filters: null checks, range validation,
and computed derived metrics like Availability_Pct. I also added Station_Status
classification (EMPTY/FULL/LOW/NORMAL) to make data immediately actionable."

---

## Key Terms to Know

| Term | Definition |
|------|-----------|
| Structured Streaming | Spark stream processing engine |
| Delta Lake | ACID storage layer on data lakes |
| Watermark | Late-data threshold for streaming |
| Checkpoint | Progress tracker for fault tolerance |
| Eventstream | Fabric real-time event router |
| OneLake | Fabric unified storage layer |
| Data Agent | AI chatbot for natural language data Q&A |
| Data Activator | Real-time alert/action engine |
| Medallion | Bronze to Silver to Gold data pattern |
| SAS Token | Shared Access Signature for Event Hub auth |

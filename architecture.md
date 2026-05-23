# Architecture Document

## Overview

This project implements a **real-time streaming data engineering pipeline** using Microsoft Fabric and Azure services, following the **Medallion Architecture** pattern.

## System Architecture

```
DATA SOURCES
  Bicycles Sample Data / Azure Event Hub
         |
         v
  FABRIC EVENTSTREAM (Real-time ingestion)
         |
         v
  LAKEHOUSE (Delta Tables)
    Bronze: events_bronze  -> Raw data, no transforms
    Silver: bikes_silver   -> Cleaned, enriched, Station_Status
    Gold:   bikes_gold     -> Neighbourhood aggregations
         |
    +---------+-----------+
    |         |           |
    v         v           v
  Data    Data        Power BI
  Agent   Activator   Dashboard
  (AI)    (Alerts)    (Visuals)
```

## Data Flow

### 1. Ingestion Layer
- **Fabric Eventstream** ingests data from sample source (Bicycles)
- Alternatively, **Azure Event Hub** can ingest from external producers
- Data arrives as JSON, parsed automatically

### 2. Storage Layer (Medallion)

| Layer | Table | Purpose | Output Mode |
|-------|-------|---------|-------------|
| Bronze | events_bronze | Raw storage | append |
| Silver | bikes_silver | Cleaned + enriched | append |
| Gold | bikes_gold | Aggregated | complete |

### 3. Processing Layer
- **Spark Structured Streaming** processes data continuously
- **Delta Lake** provides ACID transactions and checkpointing
- **Watermarking** handles late-arriving data

### 4. Consumption Layer
- **Fabric Data Agent**: Natural language Q&A over data
- **Data Activator**: Real-time alerts (email/Teams)
- **Power BI**: Interactive dashboards

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake format | ACID transactions, time travel, schema evolution |
| Medallion Architecture | Separation of concerns, data quality layers |
| Eventstream over direct Event Hub | Simpler setup, built-in Fabric integration |
| complete mode for Gold | Aggregations update as new data arrives |
| Checkpointing | Fault tolerance, exactly-once processing |
| Data Activator | Zero-code alerting, enterprise-grade |

## Scalability Considerations

- Event Hub supports millions of events/second with partitioning
- Spark Structured Streaming auto-scales with cluster size
- Delta Lake optimizes reads with Z-ordering and compaction
- Gold layer minimizes data volume for fast dashboard queries

## Security

- Fabric Data Agent uses Microsoft Entra ID authentication
- Event Hub uses Shared Access Signatures (SAS tokens)
- Connection strings should be stored in Azure Key Vault
- Data Activator respects Microsoft Purview governance policies

# Real-Time Streaming Data Engineering Pipeline | Microsoft Fabric + Azure

> An end-to-end real-time streaming data engineering project built using **Microsoft Fabric**, **Azure Event Hub**, **Spark Structured Streaming**, **Delta Lake**, and **Power BI** — following the **Medallion Architecture** (Bronze -> Silver -> Gold).

---

## Project Architecture

```
Data Sources (Fabric Eventstream / Azure Event Hub)
         |
         v
  Bronze Layer (Raw data + metadata)
         |
         v
  Silver Layer (Cleaned + Enriched + Business Logic)
         |
         v
  Gold Layer (Aggregated + Dashboard-ready)
         |
         +---> Fabric Data Agent (Natural Language Q&A)
         +---> Data Activator (Real-time Alerts)
         +---> Power BI Dashboard (Visualization)
```

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Microsoft Fabric** | Unified analytics platform |
| **Fabric Eventstream** | Real-time data ingestion |
| **Azure Event Hub** | Cloud-scale event streaming |
| **Spark Structured Streaming** | Real-time data processing |
| **Delta Lake** | ACID-compliant storage layer |
| **PySpark** | Data transformations |
| **Fabric Lakehouse** | Unified data storage |
| **Fabric Data Agent** | AI-powered natural language Q&A |
| **Data Activator** | Real-time alerts & automation |
| **Power BI** | Data visualization |
| **Python** | Event producer scripts |

---

## Medallion Architecture

### Bronze Layer (Raw)
- Stores raw streaming data exactly as received
- Adds ingestion metadata (timestamps)
- No business logic applied
- Source of truth for reprocessing

### Silver Layer (Cleaned & Enriched)
- Removes null/invalid records
- Applies data quality filters
- Adds calculated columns:
  - `Total_Docks` = No_Bikes + No_Empty_Docks
  - `Availability_Pct` = (No_Bikes / Total_Docks) x 100
  - `Station_Status` = EMPTY / FULL / LOW / NORMAL
- Adds processing timestamps

### Gold Layer (Aggregated)
- Neighbourhood-level summaries
- Average bikes available, average availability %
- Total capacity and station counts
- Ready for dashboards and business reports

---

## Project Structure

```
realtime-streaming-fabric-pipeline/
├── README.md
├── .gitignore
├── config/
│   └── project_config.py
├── notebooks/
│   ├── 01_bronze_silver_gold_streaming.py
│   ├── 02_eventstream_medallion_pipeline.py
│   ├── 03_event_hub_producer.py
│   └── 04_data_verification.py
├── docs/
│   ├── architecture.md
│   └── interview_guide.md

```

---

## Getting Started

### Prerequisites
- Microsoft Fabric account (Trial or Paid)
- Azure subscription (for Event Hub - optional)
- Fabric Workspace with Lakehouse

### Step 1: Set up Fabric Workspace
1. Go to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Create workspace: `RealTimeStreamingProject`
3. Create Lakehouse: `StreamingLakehouse`

### Step 2: Set up Eventstream
1. In workspace -> New item -> **Eventstream**
2. Name: `WeatherEventstream`
3. Add source: **Sample data** -> Bicycles
4. Add destination: **Lakehouse** -> `StreamingLakehouse` -> table: `events_bronze`
5. Publish

### Step 3: Run Medallion Pipeline
1. Create new Notebook
2. Attach `StreamingLakehouse`
3. Run code from `notebooks/02_eventstream_medallion_pipeline.py`
4. Verify tables: `events_bronze` -> `bikes_silver` -> `bikes_gold`

### Step 4: Set up AI Agent
1. New item -> **Fabric Data Agent**
2. Add `StreamingLakehouse` as data source
3. Select tables: `events_bronze`, `bikes_silver`, `bikes_gold`
4. Add instructions and example queries
5. Publish

### Step 5: Set up Alerts
1. Open Eventstream -> Edit
2. Add destination -> **Activator**
3. Create rule: `No_Bikes = 0` -> Send email alert

---

## Sample Data

The pipeline processes London bike-sharing station data:

| Column | Type | Description |
|--------|------|-------------|
| `BikepointID` | String | Unique station identifier |
| `Street` | String | Street name |
| `Neighbourhood` | String | London area |
| `Latitude` | Double | GPS latitude |
| `Longitude` | Double | GPS longitude |
| `No_Bikes` | Long | Available bikes |
| `No_Empty_Docks` | Long | Empty docking spaces |

---

## AI Components

### Fabric Data Agent
- Ask questions in **plain English**
- Agent converts to SQL automatically
- Example: *"Which neighbourhood has the lowest bike availability?"*

### Data Activator
- Monitors streaming data 24/7
- Sends email when station has 0 bikes
- Sends Teams message when availability < 20%

---

## Key Metrics (Gold Layer)

| Metric | Description |
|--------|-------------|
| `avg_bikes_available` | Average bikes per neighbourhood |
| `avg_availability_pct` | Average availability percentage |
| `total_capacity` | Total docks in area |
| `total_stations` | Number of stations |
| `min_bikes` / `max_bikes` | Range of availability |

---

## Skills Demonstrated

- Real-time streaming with Spark Structured Streaming
- Medallion Architecture (Bronze / Silver / Gold)
- Delta Lake (ACID transactions, checkpointing)
- Data quality filtering & enrichment
- Windowed aggregations & watermarking
- Event-driven architecture (Eventstream / Event Hub)
- AI integration (Fabric Data Agent - NL to SQL)
- Real-time alerting (Data Activator)
- Microsoft Fabric ecosystem

---

## Author

**Chemuduru Jayanth**

---

## License

This project is for educational and portfolio purposes.

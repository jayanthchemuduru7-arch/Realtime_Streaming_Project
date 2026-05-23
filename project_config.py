# ============================================================
# Project Configuration
# ============================================================
# Replace placeholder values with your actual details.
# NEVER commit actual secrets to GitHub!
# ============================================================

# Fabric Settings
FABRIC_WORKSPACE = "RealTimeStreamingProject"
LAKEHOUSE_NAME = "StreamingLakehouse"

# Table Names
BRONZE_TABLE = "events_bronze"
SILVER_TABLE = "bikes_silver"
GOLD_TABLE = "bikes_gold"

# Checkpoint Paths
BRONZE_CHECKPOINT = "Files/checkpoints/bronze_v2"
SILVER_CHECKPOINT = "Files/checkpoints/bikes_silver"
GOLD_CHECKPOINT = "Files/checkpoints/bikes_gold"

# Azure Event Hub (Optional)
EVENT_HUB_NAMESPACE = "your-namespace.servicebus.windows.net"
EVENT_HUB_NAME = "weather-events"

# Streaming Settings
ROWS_PER_SECOND = 5
WATERMARK_DURATION = "2 minutes"
WINDOW_DURATION = "1 minute"

# Data Quality Thresholds
MIN_VALID_TEMPERATURE = 0
MAX_VALID_TEMPERATURE = 50
LOW_AVAILABILITY_THRESHOLD = 20

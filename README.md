# velocity_railways is a leading railway operator based in the United Kingdom.
The company is dedicated to providing efficient, reliable and timely train services across the nation.

# PROBLEM STATEMENT
leaveraging cutting-edge and real-time data integration to ensure its customers receive up-to-the-minute information on train schedules, departures, and arrivals, the company is faced with challenges in providing accurate and real-time train departure information to its passengers.
Their current system provides scheduled departure times but often lacks real-time updates on delays, cancellations or other disruptions.
The company wants to create a better pipeline that fetches real time data from a source and saves it on a database for further usages.

# TECH STACK
Requests
Apache Flink
Great Expectations
Airflow
Google Cloud Storage
PostgreSQL
Python

# IMPLEMENTATION
Extraction/Ingestion Layer:
- Data pulled from train schedule APIs (endpoint: https://developer.transportapi.com/docs#GET/v3/uk/train/station_timetables/{id}.json)
- Real-time data collected through Flink

Data Validation:
- Great Expectations runs locally to validate ingested/extracted data.
- Validation results are logged and monitored before proceeding.

Data Duplication:
- Python scripts handle the duplication process, sending the data both to Google Cloud Storage and locally on PostgreSQL for backup.

Orchestration:
- Airflow locally orchestrates the full pipeline, managing the ingestion/extraction, validation and duplication of data.
 

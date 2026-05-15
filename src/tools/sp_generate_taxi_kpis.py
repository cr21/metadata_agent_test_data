import sys
import uuid
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

PROJECT_DATASET = "project-5c016d48-80d5-4534-b69.orc_dataset"
client = bigquery.Client()

def curate_raw_taxi_data(target_date: str):
    """
    Python replacement for: sp_curate_raw_taxi_data
    Extracts public NYC taxi data, cleans fields, flags distances.
    """
    print(f"Starting Data Curation Layer for date: {target_date}...")
    
    query = f"""
        SELECT vendor_id, pickup_datetime, dropoff_datetime, trip_distance, fare_amount, tip_amount, extra, mta_tax
        FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
        WHERE DATE(pickup_datetime) = DATE('{target_date}')
          AND fare_amount > 0 
          AND trip_distance > 0
    """
    raw_df = client.query(query).to_dataframe()
    
    if raw_df.empty:
        print(f"No source data located for date: {target_date}")
        return False

    # Apply transformations Pythonically
    raw_df['trip_id'] = [str(uuid.uuid4()) for _ in range(len(raw_df))]
    raw_df = raw_df.rename(columns={'fare_amount': 'base_fare_amount'})
    
    # Financial cleaning calculation
    raw_df['total_amount_clean'] = (
        raw_df['base_fare_amount'] + raw_df['extra'] + raw_df['mta_tax'] + raw_df['tip_amount']
    )
    
    # Flag business anomalies/rules
    raw_df['is_long_distance'] = raw_df['trip_distance'] > 10

    # Save to staging/curated table
    output_cols = ['trip_id', 'vendor_id', 'pickup_datetime', 'dropoff_datetime', 
                   'trip_distance', 'base_fare_amount', 'tip_amount', 'total_amount_clean', 'is_long_distance']
    
    raw_df[output_cols].to_gbq(f"{PROJECT_DATASET}.stg_curated_taxi_trips", project_id=client.project, if_exists='append')
    print(f"Successfully processed and curated {len(raw_df)} records.")
    return True


def generate_taxi_kpis(target_date: str):
    """
    Python replacement for: sp_generate_taxi_kpis
    Aggregates the curated structured staging data into a final executive summary table.
    """
    print(f"Starting Executive KPI Generation Layer for date: {target_date}...")

    # Load from our curated table layer
    query = f"""
        SELECT * FROM `{PROJECT_DATASET}.stg_curated_taxi_trips`
        WHERE DATE(pickup_datetime) = DATE('{target_date}')
    """
    curated_df = client.query(query).to_dataframe()

    if curated_df.empty:
        print("No curated data available for this date window to generate metrics.")
        return

    # Grouping and aggregating calculations
    kpi_summary = curated_df.groupby('vendor_id').agg(
        total_amount_clean_sum=('total_amount_clean', 'sum'),
        trip_distance_sum=('trip_distance', 'sum'),
        total_tips_collected=('tip_amount', 'sum'),
        base_fare_amount_sum=('base_fare_amount', 'sum')
    ).reset_index()

    # Apply calculations with safe zero division protection
    kpi_summary['report_date'] = pd.to_datetime(target_date).date()
    
    kpi_summary['avg_revenue_per_mile'] = (
        kpi_summary['total_amount_clean_sum'] / kpi_summary['trip_distance_sum']
    ).fillna(0)
    
    kpi_summary['efficiency_score'] = (
        (kpi_summary['total_tips_collected'] / kpi_summary['base_fare_amount_sum']) * 100
    ).fillna(0)
    
    kpi_summary['last_updated_at'] = datetime.utcnow()

    # Prune and organize output structure matching target schema
    final_kpi_table = kpi_summary[['report_date', 'vendor_id', 'avg_revenue_per_mile', 
                                   'total_tips_collected', 'efficiency_score', 'last_updated_at']]

    # Write metrics out to production Target summary table
    final_kpi_table.to_gbq(f"{PROJECT_DATASET}.fct_taxi_performance_kpi", project_id=client.project, if_exists='append')
    print("Executive KPIs updated and written to production successfully.")


if __name__ == "__main__":
    # Expecting parameter date execution: python taxi_pipeline.py 2022-06-15
    t_date = sys.argv[1] if len(sys.argv) > 1 else '2022-01-01'
    
    # Sequence execution enforcing structural upstream data readiness
    data_is_ready = curate_raw_taxi_data(t_date)
    if data_is_ready:
        generate_taxi_kpis(t_date)

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery

def run_customer_orders_etl(batch_id: int, load_date: str):
    # Initialize BigQuery Client
    client = bigquery.Client()
    project_dataset = "project-5c016d48-80d5-4534-b69.orc_dataset"
    
    def log_process(message: str):
        """Helper function to log processes into the BigQuery log table"""
        log_data = [{
            "log_time": datetime.utcnow().isoformat(),
            "batch_id": batch_id,
            "message": message
        }]
        try:
            client.insert_rows_json(f"{project_dataset}.etl_process_log", log_data)
        except Exception:
            pass # Ignore logging infrastructure errors per SP design

    try:
        log_process("KPI ETL Started via Python")

        # -------------------------------------------------------------------------
        # STEP 1: LOAD STAGING (Extract from External & Upsert into Staging Table)
        # -------------------------------------------------------------------------
        query_extract = f"""
            SELECT order_id, customer_id, order_amount, order_date, DATE('{load_date}') AS load_date
            FROM `{project_dataset}.external_orders`
            WHERE batch_id = {batch_id}
        """
        src_df = client.query(query_extract).to_dataframe()
        
        if src_df.empty:
            log_process("No records found for the given batch_id. Exiting successfully.")
            return

        # Simulating MERGE / UPSERT locally using pandas
        try:
            stg_df = client.query(f"SELECT * FROM `{project_dataset}.stg_customer_orders`").to_dataframe()
            # Remove existing rows in staging to prevent duplicates (mimicking MATCHED THEN UPDATE)
            stg_df = stg_df[~stg_df['order_id'].isin(src_df['order_id'])]
            stg_df = pd.concat([stg_df, src_df], ignore_index=True)
        except Exception:
            # If staging doesn't exist or is empty, use the source dataframe directly
            stg_df = src_df.copy()

        log_process(f"Loaded records to memory: {len(src_df)}")

        # -------------------------------------------------------------------------
        # STEP 2: TRANSFORM + KPI CALCULATION (Python Window Functions)
        # -------------------------------------------------------------------------
        # Ensure correct data sorting for window operations
        stg_df = stg_df.sort_values(by=['customer_id', 'order_date']).reset_index(drop=True)
        
        # Window Partition Group
        customer_group = stg_df.groupby('customer_id')

        # KPI: Previous Order Amount (LAG)
        stg_df['prev_order_amount'] = customer_group['order_amount'].shift(1)

        # KPI: Growth Percentage (with Safe Division handling)
        stg_df['growth_pct'] = (stg_df['order_amount'] - stg_df['prev_order_amount']) / stg_df['prev_order_amount']
        stg_df['growth_pct'] = stg_df['growth_pct'].replace([np.inf, -np.inf], np.nan).fillna(0)

        # KPI: 3-Order Rolling Average
        stg_df['rolling_avg_3'] = customer_group['order_amount'].transform(
            lambda x: x.rolling(window=3, min_periods=1).mean()
        )

        # KPI: Cumulative Spend (Running Total)
        stg_df['cumulative_spend'] = customer_group['order_amount'].cumsum()

        # KPI: Order Rank by Amount descending per customer (DENSE_RANK)
        stg_df['order_rank'] = stg_df.groupby('customer_id')['order_amount'].rank(method='dense', ascending=False).astype(int)
        
        # Update process flag
        stg_df['process_flag'] = 'KPI_DONE'
        
        log_process(f"KPI computed for records: {len(stg_df[stg_df['load_date'] == load_date])}")

        # Save calculations back into the staging table (Overwrite to refresh state)
        stg_df.to_gbq(f"{project_dataset}.stg_customer_orders", project_id=client.project, if_exists='replace')

        # -------------------------------------------------------------------------
        # STEP 3: FINAL MERGE INTO TARGET TABLE
        # -------------------------------------------------------------------------
        final_src = stg_df[stg_df['process_flag'] == 'KPI_DONE'].copy()
        
        # Rename columns to match target schema
        final_src = final_src.rename(columns={
            'prev_order_amount': 'prev_amount',
            'rolling_avg_3': 'rolling_avg',
            'cumulative_spend': 'total_spend',
            'order_rank': 'rank_in_customer'
        })
        
        current_time = datetime.utcnow()
        final_src['last_updated'] = current_time

        try:
            target_df = client.query(f"SELECT * FROM `{project_dataset}.customer_orders`").to_dataframe()
            # Mimic MERGE statement criteria
            target_df = target_df[~target_df['order_id'].isin(final_src['order_id'])]
            if 'created_date' not in final_src.columns:
                final_src['created_date'] = current_time
            final_output = pd.concat([target_df, final_src], ignore_index=True)
        except Exception:
            final_src['created_date'] = current_time
            final_output = final_src

        # Write final consolidated dataframe to BigQuery Target table
        columns_to_keep = ['order_id', 'customer_id', 'order_amount', 'prev_amount', 'growth_pct', 
                           'rolling_avg', 'total_spend', 'rank_in_customer', 'created_date', 'last_updated']
        final_output[columns_to_keep].to_gbq(f"{project_dataset}.customer_orders", project_id=client.project, if_exists='replace')

        # -------------------------------------------------------------------------
        # STEP 4: ERROR CAPTURE (Log negative values)
        # -------------------------------------------------------------------------
        error_records = stg_df[stg_df['order_amount'] < 0][['order_id']].copy()
        if not error_records.empty:
            error_records['error_message'] = 'Negative or invalid amount'
            error_records['error_date'] = datetime.utcnow().isoformat()
            error_records.to_gbq(f"{project_dataset}.etl_error_table", project_id=client.project, if_exists='append')

        log_process("KPI ETL Completed Successfully via Python")

    except Exception as e:
        log_process(f"FATAL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Expecting parameters: python file.py <batch_id> <load_date>
    # Example execution: python customer_orders.py 1001 2026-05-15
    b_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1001
    l_date = sys.argv[2] if len(sys.argv) > 2 else datetime.today().strftime('%Y-%m-%d')
    run_customer_orders_etl(b_id, l_date)

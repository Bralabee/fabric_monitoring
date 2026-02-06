import glob
import json
import os

import pandas as pd


def load_latest_json(pattern):
    files = glob.glob(pattern)
    if not files:
        return None, None
    latest_file = max(files, key=os.path.getctime)
    print(f"Loading data from: {latest_file}")
    with open(latest_file) as f:
        data = json.load(f)
    return pd.DataFrame(data), latest_file


def analyze_dataframe(df, item_type, source_file=None):
    if df is None or df.empty:
        print(f"No data found for {item_type}.")
        return

    print(f"\n{'='*20} {item_type} Analysis {'='*20}")

    # Basic Counts
    total_runs = len(df)
    failed_runs = len(df[df['status'] == 'Failed'])
    success_rate = ((total_runs - failed_runs) / total_runs) * 100 if total_runs > 0 else 0

    print(f"Total Runs: {total_runs}")
    print(f"Failed Runs: {failed_runs}")
    print(f"Success Rate: {success_rate:.2f}%")

    # Duration Analysis
    if 'startTimeUtc' in df.columns and 'endTimeUtc' in df.columns:
        df['startTimeUtc'] = pd.to_datetime(df['startTimeUtc'], format='mixed', errors='coerce')
        df['endTimeUtc'] = pd.to_datetime(df['endTimeUtc'], format='mixed', errors='coerce')
        df['duration'] = (df['endTimeUtc'] - df['startTimeUtc']).dt.total_seconds() / 60.0  # Minutes

        avg_duration = df['duration'].mean()
        max_duration = df['duration'].max()
        print(f"Average Duration: {avg_duration:.2f} minutes")
        print(f"Max Duration: {max_duration:.2f} minutes")

    # Failure Analysis
    if failed_runs > 0:
        print(f"\n--- Top 5 Failing {item_type}s ---")
        failing_items = df[df['status'] == 'Failed']['_item_name'].value_counts().head(5)
        print(failing_items)

        print("\n--- Top 5 Failure Reasons ---")
        # Clean up failure reasons (sometimes they are long JSON strings, take first 100 chars)
        failure_reasons = df[df['status'] == 'Failed']['failureReason'].astype(str).apply(lambda x: x[:150] + '...' if len(x) > 150 else x).value_counts().head(5)
        for reason, count in failure_reasons.items():
            print(f"({count}) {reason}")

    # Export to CSV
    if source_file:
        # Save Full CSV
        csv_path = source_file.replace('.json', '.csv')
        print(f"\nSaving full data to: {csv_path}")
        df.to_csv(csv_path, index=False)

        # Save Failures CSV
        if failed_runs > 0:
            failures_csv_path = source_file.replace('.json', '_failures.csv')
            print(f"Saving failures to: {failures_csv_path}")
            df[df['status'] == 'Failed'].to_csv(failures_csv_path, index=False)

def main():
    base_dir = 'exports/fabric_item_details'

    # Analyze Pipelines
    pipeline_df, pipeline_file = load_latest_json(os.path.join(base_dir, 'pipelines_*.json'))
    analyze_dataframe(pipeline_df, "Pipelines", pipeline_file)

    # Analyze Notebooks
    notebook_df, notebook_file = load_latest_json(os.path.join(base_dir, 'notebooks_*.json'))
    analyze_dataframe(notebook_df, "Notebooks", notebook_file)

if __name__ == "__main__":
    main()

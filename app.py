import os
import time
import pandas as pd
import smtplib
import pymysql
from pymysql.constants import CLIENT
import sys
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from colorama import Fore, init
from tabulate import tabulate
import streamlit as st

try:
    from plyer import notification
    HAS_PLYER = True
except ImportError:
    HAS_PLYER = False

init(autoreset=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SENDER_EMAIL = "anuj2804j@gmail.com"
SENDER_PASSWORD = "fsue ayla orye xlht"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "abcAnujJ1@hexaware.com"

DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'
DB_NAME = 'prototype2'
DB_TABLE = 'PipelineRuns'
STATUS_TABLE = 'currentStatus'

REPORT_DIR = os.path.join(BASE_DIR, "report")
LOG_FILE = os.path.join(BASE_DIR, "pipeline_status.json")
os.makedirs(REPORT_DIR, exist_ok=True)

pipeline_status = {}
last_checked_time = None

def log(msg, color=Fore.WHITE):
    print(color + f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def countdown(seconds):
    for i in range(seconds, 0, -1):
        sys.stdout.write(f"\rSleeping for {i} seconds...")
        sys.stdout.flush()
        time.sleep(1)
    print()

def notify(title, message):
    if HAS_PLYER:
        notification.notify(title=title, message=message, timeout=5)

def load_pipeline_log():
    global pipeline_status
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            pipeline_status = json.load(f)
            for k in pipeline_status:
                pipeline_status[k]['last_run'] = pd.to_datetime(pipeline_status[k]['last_run'])
                if 'reminder' not in pipeline_status[k]:
                    pipeline_status[k]['reminder'] = 0
    else:
        pipeline_status = {}

def save_pipeline_log():
    serializable_status = {
        k: {
            **v,
            "last_run": v['last_run'].strftime('%Y-%m-%d %H:%M:%S')
        } for k, v in pipeline_status.items()
    }
    with open(LOG_FILE, 'w') as f:
        json.dump(serializable_status, f, indent=4)

def save_collected_failures():
    unresolved = [
        {
            "PipelineName": k,
            "RunStart": v["last_run"],
            "Status": v["status"],
            "Error": v["error"],
            "RunID": v["runid"]
        }
        for k, v in pipeline_status.items() if v["status"].lower() == "failed"
    ]
    if unresolved:
        df = pd.DataFrame(unresolved)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(REPORT_DIR, f"Failure_Report_{timestamp}.csv")
        df.to_csv(report_path, index=False)
        log(f"Saved unresolved failures to {report_path}", Fore.GREEN)
    else:
        log("No unresolved failures to report.", Fore.YELLOW)

html_style = """<style>.data-table {font-family: Arial, sans-serif;border-collapse: collapse;width: 100%;} .data-table td, .data-table th {border: 1px solid #dddddd;text-align: left;padding: 8px;max-width: 300px;word-wrap: break-word;} .data-table tr:nth-child(even) {background-color: #f9f9f9;} .data-table th {background-color: #f2a2a2;color: black;}</style>"""

def send_failure_email(new_failures_df, resolved_pipelines, reminder_level=None):
    if reminder_level == 1:
        subject = f"[REMINDER] 6+ Hour Unresolved Failures"
    elif reminder_level == 2:
        subject = f"[URGENT REMINDER] 24+ Hour Unresolved Failures"
    else:
        subject = f"[ALERT] {len(new_failures_df)} Failures Detected"

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject

    if not new_failures_df.empty:
        display_df = new_failures_df.copy()
        if "Error" in display_df.columns:
            display_df["Error"] = display_df["Error"].apply(
                lambda err: f'<span title="{err}">{err[:50]}{"..." if len(err) > 50 else ""}</span>' if pd.notna(err) else ""
            )
        unresolved_table = display_df.to_html(index=False, border=0, escape=False, justify="center", classes="data-table")
    else:
        unresolved_table = "<p>No unresolved failures.</p>"

    if resolved_pipelines:
        resolved_data = [
            {
                "PipelineName": name,
                "LastFailed": v["last_run"].strftime('%Y-%m-%d %H:%M:%S'),
                "Error": v["error"],
                "RunID": v["runid"]
            } for name, v in resolved_pipelines.items()
        ]
        resolved_df = pd.DataFrame(resolved_data)
        resolved_df["Error"] = resolved_df["Error"].apply(
            lambda err: f'<span title="{err}">{err[:50]}{"..." if len(err) > 50 else ""}</span>' if pd.notna(err) else ""
        )
        resolved_table = resolved_df.to_html(index=False, border=0, escape=False, justify="center", classes="data-table")
    else:
        resolved_table = "<p>No pipelines were resolved in this session.</p>"

    html_body = f"""
    <html>
    <head>{html_style}</head>
    <body>
        <p>Hello,</p>
        <p><b>Failures:</b></p>
        {unresolved_table}
        <br/>
        <p><b>Previously Failed Pipelines That Got Resolved:</b></p>
        {resolved_table}
        <p style="margin-top:20px;">Regards,<br>Pipeline Monitor Bot</p>
    </body>
    </html>
    """

    msg.attach(MIMEText(html_body, 'html'))
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        log("Email sent.", Fore.GREEN)
        notify("Pipeline Alert", "Email sent")
    except Exception as e:
        log(f"Email failed: {e}", Fore.RED)

def ensure_current_status_table_exists():
    try:
        # Connect to MySQL server (not specifying DB initially)
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            client_flag=CLIENT.MULTI_STATEMENTS
        )
        with connection.cursor() as cursor:
            # Ensure the database exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
            cursor.execute(f"USE {DB_NAME};")

            # Check if the table exists
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{DB_NAME}' AND table_name = '{STATUS_TABLE}';
            """)
            if cursor.fetchone()[0] == 0:
                # Create the table if not exists
                create_table_sql = f"""
                CREATE TABLE {STATUS_TABLE} (
                    PipelineName VARCHAR(50),
                    RunStart DATETIME,
                    RunEnd DATETIME,
                    Duration VARCHAR(20),
                    TriggeredBy VARCHAR(50),
                    Status VARCHAR(20),
                    Error TEXT,
                    Run VARCHAR(50),
                    Parameters TEXT,
                    Annotations TEXT,
                    RunID VARCHAR(50)
                );
                """
                cursor.execute(create_table_sql)
                print(f"✅ Table `{STATUS_TABLE}` created.")
                update_current_status_with_latest_runs()
            else:
                print(f"ℹ️ Table `{STATUS_TABLE}` already exists.")
        connection.commit()
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

def update_current_status_with_latest_runs():
    try:
        # Connect to the database
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            client_flag=CLIENT.MULTI_STATEMENTS
        )

        with connection.cursor() as cursor:
            # Step 1: Get all unique pipeline names
            cursor.execute(f"SELECT DISTINCT PipelineName FROM {DB_TABLE}")
            pipelines = cursor.fetchall()

            # Step 2: Clear currentStatus table before repopulating
            cursor.execute(f"DELETE FROM {STATUS_TABLE}")

            # Step 3: Insert latest entry for each pipeline
            for (pipeline_name,) in pipelines:
                cursor.execute(f"""
                    SELECT *
                    FROM {DB_TABLE}
                    WHERE PipelineName = %s
                    ORDER BY RunStart DESC
                    LIMIT 1
                """, (pipeline_name,))
                row = cursor.fetchone()

                if row:
                    placeholders = ', '.join(['%s'] * len(row))
                    insert_sql = f"INSERT INTO {STATUS_TABLE} VALUES ({placeholders})"
                    cursor.execute(insert_sql, row)

            connection.commit()
            print("✅ currentStatus table successfully updated.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

import pymysql

def update_latest_status(df):
    try:
        # Ensure RunStart is datetime
        df['RunStart'] = pd.to_datetime(df['RunStart'])

        # Connect to the database
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        with connection.cursor() as cursor:
            for pipeline in df['PipelineName'].unique():
                # Get the latest entry for this pipeline from new data
                latest_row = df[df['PipelineName'] == pipeline].sort_values('RunStart', ascending=False).iloc[0]
                values = tuple(latest_row.values)

                # Check if the pipeline already exists in currentStatus
                cursor.execute(f"SELECT COUNT(*) FROM currentStatus WHERE PipelineName = %s", (pipeline,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Update existing row
                    update_sql = """
                        UPDATE currentStatus SET
                            RunStart = %s, RunEnd = %s, Duration = %s, TriggeredBy = %s,
                            Status = %s, Error = %s, Run = %s, Parameters = %s,
                            Annotations = %s, RunID = %s
                        WHERE PipelineName = %s
                    """
                    update_values = values[1:] + (pipeline,)  # all fields except PipelineName, then use it in WHERE
                    cursor.execute(update_sql, update_values)
                else:
                    # Insert new row
                    insert_sql = """
                        INSERT INTO currentStatus (
                            PipelineName, RunStart, RunEnd, Duration, TriggeredBy,
                            Status, Error, Run, Parameters, Annotations, RunID
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, values)

            connection.commit()
            print("✅ currentStatus table updated with latest entries.")
    except Exception as e:
        print(f"❌ Error while updating currentStatus: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

def check_new_runs():
    global last_checked_time
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        query = f"SELECT * FROM {DB_TABLE}"
        if last_checked_time:
            query += f" WHERE RunStart > '{last_checked_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        df = pd.read_sql(query, connection)
        if not df.empty:
            df['RunStart'] = pd.to_datetime(df['RunStart'])
            last_checked_time = df['RunStart'].max()
        return df
    except Exception as e:
        log(f"Database error: {e}", Fore.RED)
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

def process_runs(df):
    new_failures = []
    resolved_pipelines = {}

    for _, row in df.iterrows():
        name = row['PipelineName']
        status = row['Status'].strip().lower()
        run_time = row['RunStart']
        error = row['Error']
        runid = row['RunID']

        prev = pipeline_status.get(name)
        if status == "failed":
            pipeline_status[name] = {
                "status": "Failed",
                "last_run": run_time,
                "error": error,
                "runid": runid,
                "reminder": 0
            }
            if not prev or prev["status"].lower() != "failed":
                new_failures.append(row)
        elif status == "succeeded":
            if prev and prev["status"].lower() == "failed":
                resolved_pipelines[name] = prev
            pipeline_status[name] = {
                "status": "Succeeded",
                "last_run": run_time,
                "error": "",
                "runid": runid,
                "reminder": 0
            }

    if new_failures or resolved_pipelines:
        send_failure_email(pd.DataFrame(new_failures), resolved_pipelines)

def update_current_status_table(df):
    if df.empty:
        return

    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()

        # Loop over unique pipeline names
        for pipeline_name in df['PipelineName'].unique():
            latest_record = df[df['PipelineName'] == pipeline_name].sort_values(by='RunStart', ascending=False).iloc[0]

            # Check if this pipeline exists in currentStatus
            cursor.execute(f"SELECT COUNT(*) FROM {STATUS_TABLE} WHERE PipelineName = %s", (pipeline_name,))
            exists = cursor.fetchone()[0] > 0

            if exists:
                # UPDATE existing pipeline row
                update_query = f"""
                UPDATE {STATUS_TABLE} SET
                    RunStart = %s,
                    RunEnd = %s,
                    Duration = %s,
                    TriggeredBy = %s,
                    Status = %s,
                    Error = %s,
                    Run = %s,
                    Parameters = %s,
                    Annotations = %s,
                    RunID = %s
                WHERE PipelineName = %s
                """
                cursor.execute(update_query, (
                    latest_record['RunStart'], latest_record['RunEnd'], latest_record['Duration'],
                    latest_record['TriggeredBy'], latest_record['Status'], latest_record['Error'],
                    latest_record['Run'], latest_record['Parameters'], latest_record['Annotations'],
                    latest_record['RunID'], pipeline_name
                ))
            else:
                # INSERT new pipeline row
                insert_query = f"""
                INSERT INTO {STATUS_TABLE} (
                    PipelineName, RunStart, RunEnd, Duration, TriggeredBy, Status,
                    Error, Run, Parameters, Annotations, RunID
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    latest_record['PipelineName'], latest_record['RunStart'], latest_record['RunEnd'],
                    latest_record['Duration'], latest_record['TriggeredBy'], latest_record['Status'],
                    latest_record['Error'], latest_record['Run'], latest_record['Parameters'],
                    latest_record['Annotations'], latest_record['RunID']
                ))

        connection.commit()

    except Exception as e:
        log(f"Failed to update currentStatus: {e}", Fore.RED)
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

def check_reminders_and_send():
    now = pd.Timestamp.now()
    reminders_to_send = []

    for name, info in pipeline_status.items():
        if info["status"].lower() == "failed":
            hours_passed = (now - info["last_run"]).total_seconds() / 3600
            reminder_level = info.get("reminder", 0)
            if 6 <= hours_passed < 24 and reminder_level < 1:
                reminders_to_send.append((name, info, 1))
            elif hours_passed >= 24 and reminder_level < 2:
                reminders_to_send.append((name, info, 2))

    if reminders_to_send:
        df_data = [
            {
                "PipelineName": name,
                "RunStart": info["last_run"],
                "Status": "Failed",
                "Error": info["error"],
                "RunID": info["runid"]
            }
            for name, info, _ in reminders_to_send
        ]
        reminder_df = pd.DataFrame(df_data)
        max_level = max(lvl for _, _, lvl in reminders_to_send)
        send_failure_email(reminder_df, resolved_pipelines={}, reminder_level=max_level)

        for name, _, lvl in reminders_to_send:
            pipeline_status[name]["reminder"] = lvl

if __name__ == "__main__":

    st.set_page_config(page_title="ADF Logs Viewer", layout="wide")
    st.title("🔍 Azure Data Factory Toolkit")

    #Check and create status table if doesnt exists, also update latest log rows
    ensure_current_status_table_exists()

    load_pipeline_log()
    log("Starting intelligent pipeline monitor...", Fore.CYAN)
    try:
        while True:
            log("\nChecking for new database runs...", Fore.CYAN)
            df = check_new_runs()
            if not df.empty:
                log(f"{len(df)} new rows fetched.", Fore.BLUE)
                process_runs(df)
                update_current_status_table(df)
                update_latest_status(df)
            else:
                log("No new runs.", Fore.GREEN)
            check_reminders_and_send()
            save_pipeline_log()
            countdown(60)
    except KeyboardInterrupt:
        log("\nTermination detected. Generating final report...", Fore.YELLOW)
        save_collected_failures()
        log("Shutdown complete.", Fore.CYAN)
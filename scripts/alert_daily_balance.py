# scripts/alert_daily_balance.py
import os
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector as sf

# optional slack
SLACK_AVAILABLE = True
try:
    from slack_sdk import WebClient
    SLACK_AVAILABLE = True
except Exception:
    SLACK_AVAILABLE = False

load_dotenv()

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
ROLE      = os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER")
DB        = os.environ.get("SNOWFLAKE_DATABASE", "DEEL_ANALYTICS")
WH        = os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORMING")
SUFFIX    = os.environ.get("SCHEMA_SUFFIX", "")
SCHEMA_M  = os.environ.get("SNOWFLAKE_SCHEMA_MARTS", "MARTS") + SUFFIX

ALERT_DATE_ENV = os.environ.get("ALERT_DATE", "").strip()
THRESHOLD_PCT  = float(os.environ.get("ALERT_THRESHOLD_PCT", "0.5"))
MIN_ABS_DELTA  = float(os.environ.get("ALERT_MIN_ABSOLUTE_DELTA", "1000"))

CONSOLE_ONLY   = os.environ.get("ALERT_CONSOLE_ONLY", "1") == "1"
SLACK_TOKEN    = os.environ.get("SLACK_WEBHOOK_TOKEN", "").strip()
SLACK_CHANNEL  = os.environ.get("SLACK_CHANNEL_ID", "").strip()

def sf_connect():
    return sf.connect(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        role=ROLE,
        warehouse=WH,
        database=DB,
    )

def get_alert_date(cur) -> str:
    """
    Return YYYY-MM-DD.
    If ALERT_DATE env is set, use it; else pick max("DATE") from MART_DAILY_ORG_BALANCE.
    """
    if ALERT_DATE_ENV:
        try:
            _ = datetime.strptime(ALERT_DATE_ENV, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(f"ALERT_DATE must be YYYY-MM-DD, got: {ALERT_DATE_ENV}")
        return ALERT_DATE_ENV

    cur.execute(
        f'SELECT TO_CHAR(MAX("DATE"), \'YYYY-MM-DD\') '
        f'FROM "{DB}"."{SCHEMA_M}"."MART_DAILY_ORG_BALANCE"'
    )
    row = cur.fetchone()
    if not row or not row[0]:
        raise SystemExit("No rows in MART_DAILY_ORG_BALANCE to infer ALERT_DATE.")
    return row[0]

def fetch_alert_rows(cur, alert_date: str):
    """
    Use the mart (already includes PREV_DAILY_PAYMENT_AMOUNT & DOD_CHANGE_PCT).
    Trigger when:
      - abs USD delta ≥ MIN_ABS_DELTA, and
      - pct change is either null (no prior / prev=0) OR exceeds THRESHOLD_PCT (pos/neg).
    """
    sql = f"""
with cfg as (
  select
    to_date('{alert_date}')   as alert_date,
    {THRESHOLD_PCT}::float    as threshold_pct,
    {MIN_ABS_DELTA}::float    as min_abs
)
select
  m."ORGANIZATION_ID",
  m."DATE",
  m."DAILY_PAYMENT_AMOUNT",
  m."PREV_DAILY_PAYMENT_AMOUNT",
  m."DOD_CHANGE_PCT" as PCT_CHANGE,
  (m."DAILY_PAYMENT_AMOUNT" - coalesce(m."PREV_DAILY_PAYMENT_AMOUNT", 0)) as ABS_CHANGE
from "{DB}"."{SCHEMA_M}"."MART_DAILY_ORG_BALANCE" m, cfg
where m."DATE" = cfg.alert_date
  and (
        (ABS_CHANGE >= cfg.min_abs and (PCT_CHANGE is null or PCT_CHANGE >= cfg.threshold_pct))
     or (ABS_CHANGE <= -cfg.min_abs and (PCT_CHANGE is null or PCT_CHANGE <= -cfg.threshold_pct))
  )
order by abs(ABS_CHANGE) desc, m."ORGANIZATION_ID";
"""
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return cols, cur.fetchall()

def post_to_slack(rows, alert_date):
    if not (SLACK_AVAILABLE and SLACK_TOKEN and SLACK_CHANNEL):
        return False, "Slack not configured; set SLACK_WEBHOOK_TOKEN and SLACK_CHANNEL_ID."
    client = WebClient(token=SLACK_TOKEN)
    if not rows:
        txt = f":white_check_mark: No Daily Balance alerts for {alert_date} (threshold={THRESHOLD_PCT:.2f}, min_abs={MIN_ABS_DELTA:,.0f})."
        client.chat_postMessage(channel=SLACK_CHANNEL, text=txt)
        return True, "Posted empty alert."

    lines = [f"*Daily Balance alerts* for `{alert_date}`  (threshold={THRESHOLD_PCT:.2f}, min_abs={MIN_ABS_DELTA:,.0f})"]
    for r in rows[:50]:  # cap message size
        org_id, date_str, amt, prev, pct, absd = r
        pct_txt = "n/a" if pct is None else f"{pct*100:.1f}%"
        lines.append(f"• org `{org_id}` | Δ={absd:,.2f} ({pct_txt}) | today={amt:,.2f} prev={prev or 0:,.2f}")
    text = "\n".join(lines)
    client.chat_postMessage(channel=SLACK_CHANNEL, text=text)
    return True, f"Posted {len(rows)} rows."

def main():
    con = sf_connect()
    cur = con.cursor()
    try:
        alert_date = get_alert_date(cur)
        cols, rows = fetch_alert_rows(cur, alert_date)

        if CONSOLE_ONLY or not SLACK_TOKEN or not SLACK_CHANNEL:
            print(f"[alert_daily_balance] ALERT_DATE={alert_date}  threshold={THRESHOLD_PCT}  min_abs={MIN_ABS_DELTA}")
            if not rows:
                print("No rows triggered.")
            else:
                header = ["ORGANIZATION_ID", "DATE", "DAILY_PAYMENT_AMOUNT", "PREV_DAILY_PAYMENT_AMOUNT", "PCT_CHANGE", "ABS_CHANGE"]
                print("\t".join(header))
                for r in rows:
                    org_id, date_str, amt, prev, pct, absd = r
                    pct_txt = "n/a" if pct is None else f"{pct*100:.1f}%"
                    print(f"{org_id}\t{date_str}\t{amt:.2f}\t{(prev or 0):.2f}\t{pct_txt}\t{absd:.2f}")
        else:
            ok, msg = post_to_slack(rows, alert_date)
            print(f"Slack: {ok} - {msg}")
    finally:
        try:
            cur.close()
            con.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()

import os
import boto3
import logging
import base64
from datetime import datetime
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ===== CONFIG =====

# Bucket z raportami
BUCKET = os.getenv("REPORT_BUCKET", "motointel-cepik-raw-prod")

# Klucz bazowy dla raportów XLSX
REPORT_PREFIX = os.getenv("REPORT_PREFIX", "reports")

# Region SES (najczęściej eu-west-1, bo eu-north-1 nie ma SES)
SES_REGION = os.getenv("SES_REGION", "eu-west-1")

# Adres nadawcy (musi być zweryfikowany w SES)
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "Motobi CEPIK <no-reply@twojadomena.pl>")

# Lista odbiorców (rozdzielona przecinkami w env, np. "arek@...,ktoś@...")
EMAIL_RECIPIENTS = [
    addr.strip() for addr in os.getenv("EMAIL_RECIPIENTS", "twoj.mail@twojadomena.pl").split(",")
    if addr.strip()
]

# Klienty
s3 = boto3.client("s3")
ses = boto3.client("ses", region_name=SES_REGION)


def lambda_handler(event, context):
    """
    Po zakończeniu pipeline'u:
    - sprawdza raport XLSX w S3
    - generuje pre-signed URL
    - wysyła mail HTML z linkiem + załącznikiem
    - jak SES padnie, loguje błąd i NIE wywala pipeline'u
    """
    snapshot_date = (event or {}).get("snapshot_date") or datetime.utcnow().strftime("%Y-%m-%d-%H%M")
    logger.info(f"[NOTIFY] snapshot_date={snapshot_date}")

    report_key = f"{REPORT_PREFIX}/report-{snapshot_date}.xlsx"
    logger.info(f"[NOTIFY] expected report key: s3://{BUCKET}/{report_key}")

    report_url = None
    report_bytes = None
    report_exists = False

    # 1) Sprawdź, czy raport istnieje + pobierz
    try:
        s3.head_object(Bucket=BUCKET, Key=report_key)
        report_exists = True

        # pre-signed URL na 7 dni
        report_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": report_key},
            ExpiresIn=7 * 24 * 3600,
        )

        obj = s3.get_object(Bucket=BUCKET, Key=report_key)
        report_bytes = obj["Body"].read()

        logger.info(f"[NOTIFY] report exists. presigned_url={report_url}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(f"[NOTIFY] report file not found: s3://{BUCKET}/{report_key}")
        else:
            logger.warning(f"[NOTIFY] head/get_object error for {report_key}: {e}")

    # 2) Jeśli raport nie istnieje → log i wyjście (nie wysyłamy pustego maila)
    if not report_exists:
        summary = {
            "status": "OK_NO_REPORT",
            "snapshot_date": snapshot_date,
            "report_key": report_key,
            "report_url": None,
            "report_exists": False,
        }
        logger.info(f"[NOTIFY] summary (no report): {summary}")
        return summary

    # 3) Zbuduj ładnego maila HTML z linkiem + załącznikiem
    subject = f"Motobi CEPIK – snapshot {snapshot_date} gotowy ✅"

    body_html = f"""
    <html>
      <body>
        <p>Cześć,</p>
        <p>
          Pipeline CEPIK zakończył się poprawnie.<br/>
          Snapshot: <b>{snapshot_date}</b>
        </p>
        <p>
          Możesz pobrać raport z tego linku:<br/>
          <a href="{report_url}">{report_url}</a>
        </p>
        <p>
          Raport XLSX jest też dołączony w załączniku do tej wiadomości.
        </p>
        <hr/>
        <p style="font-size: 12px; color: #888;">
          Motobi CEPIK – automatyczna notyfikacja z AWS.
        </p>
      </body>
    </html>
    """

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)

    # Ciało HTML
    msg.attach(MIMEText(body_html, "html"))

    # Załącznik XLSX
    attachment = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    attachment.set_payload(report_bytes)
    encoders.encode_base64(attachment)
    attachment.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"motobi-cepik-report-{snapshot_date}.xlsx"
    )
    msg.attach(attachment)

    # 4) Wyślij maila przez SES
    try:
        response = ses.send_raw_email(
            Source=EMAIL_SENDER,
            Destinations=EMAIL_RECIPIENTS,
            RawMessage={"Data": msg.as_string()},
        )
        logger.info(f"[NOTIFY] SES send_raw_email ok, MessageId={response.get('MessageId')}")
        status = "OK_EMAIL_SENT"
    except ClientError as e:
        logger.error(f"[NOTIFY] SES send_raw_email failed: {e}")
        status = "WARN_EMAIL_FAILED"

    result = {
        "status": status,
        "snapshot_date": snapshot_date,
        "report_key": report_key,
        "report_url": report_url,
        "report_exists": report_exists,
    }

    logger.info(f"[NOTIFY] summary: {result}")
    return result
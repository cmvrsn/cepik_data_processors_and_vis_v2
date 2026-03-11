import json
from datetime import datetime


def _build_partitions_to_clear(start_year: int, end_year: int, month_start: int, month_end: int, now: datetime):
    """
    Build list of RAW archive partitions that should be removed after worker load.

    We clear:
    - months in the future (relative to current UTC month),
    - current month (treated as incomplete snapshot month).

    This ensures downstream PROD is based only on complete historical months.
    """
    partitions = []
    for year in range(start_year, end_year + 1):
        for month in range(month_start, month_end + 1):
            is_future_year = year > now.year
            is_current_or_future_month_in_current_year = year == now.year and month >= now.month

            if is_future_year or is_current_or_future_month_in_current_year:
                partitions.append({"year": year, "month": month})

    return partitions


def lambda_handler(event, context):
    """
    motobi-plan-snapshot
    ---------------------
    Ta lambda tworzy pełny plan snapshotu, ale PRIORYTETEM jest event startowy.
    Jeżeli użytkownik poda:
        - start_year
        - end_year
        - month_start
        - month_end
    to używamy tych wartości.

    Jeżeli czegoś nie poda — stosujemy defaulty.
    """

    # 1️⃣ Pobieramy wartości z eventu (priorytet – to jest to, czego chcesz!)
    start_year = int(event.get("start_year", 2005))
    end_year = int(event.get("end_year", datetime.utcnow().year))
    month_start = int(event.get("month_start", 1))
    month_end = int(event.get("month_end", 12))

    # Lata do listy (pracuje z tym worker)
    years = list(range(start_year, end_year + 1))

    now = datetime.utcnow()

    # 2️⃣ Snapshot date — unikalny dla każdego uruchomienia
    snapshot_date = now.strftime("%Y-%m-%d-%H%M")

    # 3️⃣ Build cleanup list for incomplete/future partitions in current run scope
    partitions_to_clear = _build_partitions_to_clear(
        start_year=start_year,
        end_year=end_year,
        month_start=month_start,
        month_end=month_end,
        now=now,
    )

    # 4️⃣ Budujemy plan
    plan = {
        "snapshot_date": snapshot_date,
        "years": years,
        "partitions_to_clear": partitions_to_clear,

        # kluczowe → dla Step Functions
        "year_start": start_year,
        "year_end": end_year,
        "month_start": month_start,
        "month_end": month_end,

        "cluster": "arn:aws:ecs:eu-north-1:976193233554:cluster/motointel-cepik",
        "task_definition": "arn:aws:ecs:eu-north-1:976193233554:task-definition/motointel-cepik-worker",
        "container_name": "cepik-worker",

        "s3_bucket": "motointel-cepik-raw-prod",
        "s3_prefix": "snapshots"
    }

    # OutputPath w Step Functions to $.plan → musimy zwrócić {"plan": plan}
    return {"plan": plan}

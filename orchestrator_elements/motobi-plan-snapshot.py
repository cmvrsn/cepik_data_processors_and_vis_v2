import json
from datetime import datetime


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

    Workaround: wyliczamy partitions_to_clear dla bieżącego roku, aby po workerze
    usunąć z nowego snapshotu miesiące > poprzedni miesiąc (oraz puste prefixy),
    bez zmian w workerze.
    """

    # 1️⃣ Pobieramy wartości z eventu (priorytet – to jest to, czego chcesz!)
    start_year = int(event.get("start_year", 2005))
    end_year = int(event.get("end_year", datetime.utcnow().year))
    month_start = int(event.get("month_start", 1))
    month_end = int(event.get("month_end", 12))

    # Lata do listy (pracuje z tym worker)
    years = list(range(start_year, end_year + 1))

    # 2️⃣ Snapshot date — unikalny dla każdego uruchomienia
    now = datetime.utcnow()
    snapshot_date = now.strftime("%Y-%m-%d-%H%M")

    # 3️⃣ Workaround cleanup plan
    # target_month = poprzedni miesiąc względem daty runu
    if now.month == 1:
        target_year = now.year - 1
        target_month = 12
    else:
        target_year = now.year
        target_month = now.month - 1

    partitions_to_clear = []
    # Gdy zakres obejmuje bieżący rok runu, czyścimy miesiące > target_month
    # (np. start 8 marca 2026 -> usuwamy 2026/03..12, żeby zostawić max do lutego)
    if start_year <= target_year <= end_year:
        clear_from = max(month_start, target_month + 1)
        clear_to = month_end
        for m in range(clear_from, clear_to + 1):
            partitions_to_clear.append({"year": target_year, "month": f"{m:02}"})

    # 4️⃣ Budujemy plan
    plan = {
        "snapshot_date": snapshot_date,
        "years": years,

        # kluczowe → dla Step Functions
        "year_start": start_year,
        "year_end": end_year,
        "month_start": month_start,
        "month_end": month_end,

        "cluster": "arn:aws:ecs:eu-north-1:976193233554:cluster/motointel-cepik",
        "task_definition": "arn:aws:ecs:eu-north-1:976193233554:task-definition/motointel-cepik-worker",
        "container_name": "cepik-worker",

        "s3_bucket": "motointel-cepik-raw-prod",
        "s3_prefix": "snapshots",

        # nowy element workaroundu
        "partitions_to_clear": partitions_to_clear,
    }

    # OutputPath w Step Functions to $.plan → musimy zwrócić {"plan": plan}
    return {"plan": plan}

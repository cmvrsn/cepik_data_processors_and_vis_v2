# CEPIK Pipeline — High-Level Production Readiness Audit

## Czy to jest data lake?
Tak — obecna architektura spełnia cechy data lake:
- warstwa RAW w S3 (`snapshots/archive/...`) zapisywana jako pliki parquet,
- warstwa „latest” publikowana atomowo do osobnego prefixu,
- warstwa agregatów/serving (PROD + trend + top brand MoM) budowana przez Athena,
- orkiestracja całego przepływu przez Step Functions.

W praktyce to **lakehouse-lite** (S3 + Athena + curated tables), ale nazwa „datalake” jest jak najbardziej uzasadniona.

## Co jest zrobione dobrze (engineering strengths)
1. **Czytelna orkiestracja sekwencyjna** — jawne kroki pipeline (plan → ingest → publish latest → build prod → QA → dim refresh → history repair → trend/MoM).
2. **Idempotency pattern** w agregatach snapshotowych (np. trend/MoM) — ochrona przed duplikacją danych.
3. **Rozdzielenie warstw danych** (`raw archive`, `latest`, `prod`, `history aggregates`) — dobra baza pod audytowalność.
4. **Walidacja jakości (QA)** w pipeline — porównanie RAW vs PROD przed krokami downstream.
5. **Oddzielenie API serving od pipeline** — endpointy czytają gotowe tabele, zamiast uruchamiać kosztowne zapytania ad hoc dla każdego requestu.

## Co trzeba sprawdzić przed testami integracyjnymi (checklist)
1. **Kontrakty schematów i kompatybilność typów**
   - zgodność kolumn między `raw_archive`, `motobi_raw_latest`, `motobi_prod_latest`, tabelami trend/MoM,
   - stabilność typów numerycznych (`vehicle_count`, `mom_delta_abs`, `mom_delta_pct`).
2. **Idempotencja end-to-end**
   - ponowne uruchomienie tego samego `snapshot_date` nie duplikuje rekordów,
   - scenariusze retry/partial failure dla kroków pośrednich.
3. **Jakość i poprawność logiki biznesowej**
   - filtry biznesowe w PROD/trend są spójne z KPI dashboardu,
   - MoM dla pierwszego snapshotu i przypadków `prev_count=0` (NULL/0) jest świadomie zdefiniowane.
4. **Wydajność i koszty Athena/S3**
   - czas i koszt CTAS/INSERT dla miesięcznego wolumenu,
   - skuteczność partycjonowania i pruning partycji.
5. **Bezpieczeństwo i dostęp**
   - minimalne IAM dla Lambd/ECS,
   - szyfrowanie danych w S3 i wyników Athena,
   - kontrola dostępu do endpointów API.
6. **Obserwowalność operacyjna**
   - metryki + alarmy (Step Functions failures, Athena failures, brak raportu, anomalia QA ratio),
   - spójne correlation IDs / snapshot_date w logach.
7. **Backfill i odtwarzalność**
   - procedura backfill historycznych snapshotów,
   - procedura reprocessingu po błędzie bez ręcznej ingerencji w tabele produkcyjne.

## Co wymaga poprawy PRZED testami (must-fix / high priority)
1. **Brak formalnych testów automatycznych** dla nowych elementów orchestratora i API.
2. **Brak jawnych progów QA jako gate** (obecnie QA loguje statystyki; decyzja pass/fail powinna być zdefiniowana).
3. **Brak runbooków operacyjnych** (retry policy, rollback, incident response, backfill SOP).
4. **Brak twardego kontraktu API** dla nowego endpointu MoM (schemat odpowiedzi + edge cases).

## Co można poprawić po testach (next improvements)
1. Dodać data quality framework (np. reguły completeness/freshness/consistency).
2. Dodać pełny SLA/SLO dla dostępności pipeline i latencji od snapshotu do API.
3. Rozważyć format tabel transakcyjnych (Iceberg/Hudi/Delta) dla łatwiejszego merge/upsert i time-travel.
4. Ustandaryzować konfigurację przez ENV/SSM (mniej hardcodowanych ARN/pathów).

## Ocena gotowości
- **Potencjał produkcyjny: TAK**.
- **Gotowość „go-live dziś”: WARUNKOWA** — po domknięciu punktów must-fix (testy, QA gates, runbooki, kontrakt API).

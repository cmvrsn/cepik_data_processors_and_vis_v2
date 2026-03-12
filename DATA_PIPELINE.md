# MotoIntel Data Pipeline (CEPiK)

## Podsumowanie

Pipeline Step Functions uruchamia sekwencyjny proces budowy snapshotu CEPiK:
1. planuje zakres,
2. pobiera pełny snapshot do archiwum S3,
3. publikuje `raw_latest`,
4. buduje warstwy analityczne (PROD, trend, dim),
5. liczy Top Brand MoM i zapisuje wynik do DynamoDB,
6. wysyła notyfikację z raportem XLSX.

Orkiestracja jest liniowa (bez gałęzi równoległych) i kończy się krokiem `Send Notification`.

---

## Definicja orchestratora (kolejność kroków)

Źródło: `data_download_orchestrator/orchestrator.json`.

1. **Plan Snapshot** (`motobi-plan-snapshot`, Lambda)
2. **Run Single Full Snapshot Worker** (ECS Fargate sync)
3. **Clear RAW Partitions (Workaround)** (`motobi-clear-raw-partitions`, Lambda)
4. **Publish RAW** (`motobi-publish-latest-raw`, Lambda)
5. **Build PROD** (ECS Fargate sync, task `motobi-build-prod-latest`)
6. **QA Validate** (`motobi-qa-validate`, Lambda)
7. **Refresh DIM Brand** (`motobi-refresh-dim-brand`, Lambda)
8. **Repair RAW Archive** (ECS Fargate sync, task `motobi-repair-raw-archive`)
9. **Build Snapshot Trend** (ECS Fargate sync, task `motobi-trend-builder`)
10. **Build Top Brand MoM** (`motobi-build-top-brands-mom`, Lambda)
11. **Send Notification** (`motobi-notify`, Lambda, `End: true`)

---

## Szczegóły kroków (co czyta / co zapisuje)

### 1) Plan Snapshot — `orchestrator_elements/motobi-plan-snapshot.py`
- Buduje obiekt `plan` na bazie input eventu (`start_year`, `end_year`, `month_start`, `month_end`).
- Generuje `snapshot_date` (`%Y-%m-%d-%H%M`).
- Zwraca konfigurację pod ECS worker i S3 (`s3_bucket`, `s3_prefix`).

### 2) Run Single Full Snapshot Worker — `orchestrator_elements/run-single-full-snapshot-worker.py`
- Pobiera dane z API CEPiK strona po stronie (retry HTTP + sanity retry).
- Zapisuje parquety do archiwum S3 pod `snapshots/archive/snapshot_date=.../year=.../month=.../type=.../wojewodztwo=.../`.
- Generuje raport XLSX i zapisuje do `reports/report-<snapshot_date>.xlsx`.

### 3) Clear RAW Partitions — `orchestrator_elements/motobi-clear-raw-partitions.py`
- Opcjonalny cleanup wskazanych partycji snapshotu (na podstawie `partitions_to_clear`).
- Usuwa obiekty S3 pod prefiksami `snapshots/archive/snapshot_date=.../year=.../month=.../`.

### 4) Publish RAW — `orchestrator_elements/motobi-publish-latest-raw.py`
- Czyści `snapshots/latest/`.
- Kopiuje cały prefiks bieżącego snapshotu z `archive/snapshot_date=.../` do `latest/`.

### 5) Build PROD — `orchestrator_elements/motobi-build-prod-latest.py`
- Athena (`motobi_cepik`):
  - `MSCK REPAIR TABLE motobi_raw_latest`,
  - `DROP TABLE IF EXISTS motobi_prod_latest`,
  - CTAS `motobi_prod_latest` do `s3://motointel-cepik-raw-prod/prod-data/latest/`.
- Zawiera filtry biznesowe i agregację `COUNT(DISTINCT id)`.

### 6) QA Validate — `orchestrator_elements/motobi-qa-validate.py`
- Athena (`motobi_cepik`):
  - liczy `COUNT(*)` w `motobi_raw_latest`,
  - liczy `SUM(total_count)` w `motobi_prod_latest`.
- Zwraca metryki QA (`diff`, `ratio_prod_to_raw`), nie przerywa pipeline na thresholdach.

### 7) Refresh DIM Brand — `orchestrator_elements/motobi-refresh-dim-brand.py`
- Rebuild tabeli `dim_brand` z `motobi_raw_latest`.
- Kasuje i odtwarza tabelę external w `s3://motointel-cepik-raw-prod/dim/brand/`.

### 8) Repair RAW Archive — `orchestrator_elements/motobi-repair-raw-archive.py`
- Skanuje fizyczne partycje w S3 dla `snapshot_date`.
- Rejestruje brakujące partycje w Athena (`ALTER TABLE raw_archive ADD IF NOT EXISTS PARTITION ...`).

### 9) Build Snapshot Trend — `orchestrator_elements/motobi-build-snapshot-trend.py`
- Dla `snapshot_month = snapshot_date[:7]`:
  - sprawdza idempotencję w `motobi_prod_snapshot_trend`,
  - robi `INSERT INTO ... SELECT ...` z `raw_archive`.
- Dodaje trend tylko raz na miesiąc snapshotu.

### 10) Build Top Brand MoM — `orchestrator_elements/motobi-build-top-brand-mom.py`
- Liczy Top 20 marek wg logiki snapshot-vs-snapshot:
  - bieżąca wartość = liczba aut marki w **bieżącym snapshotcie** dla miesiąca rejestracji `snapshot_month - 1`,
  - wartość porównawcza = liczba aut marki w **poprzednim snapshotcie** dla miesiąca `snapshot_month - 2`.
- Wykorzystuje `raw_archive` jako źródło historyczne.
- Zapisuje wynik (upsert) do DynamoDB (`TOP_BRAND_MOM_DDB_TABLE`, domyślnie `motobi_top_brand_mom`) z kluczem:
  - `PK: snapshot_date`
  - `SK: brand`
- Obsługuje fallback workgroup Athena (`ATHENA_WORKGROUP` + `ATHENA_FALLBACK_WORKGROUP`) przy błędach Managed Results.

### 11) Send Notification — `orchestrator_elements/motobi-notify.py`
- Sprawdza raport XLSX w S3 (`reports/report-<snapshot_date>.xlsx`).
- Generuje pre-signed URL (7 dni).
- Wysyła mail przez SES z linkiem i załącznikiem.
- Jeśli raportu nie ma: zwraca `OK_NO_REPORT` (bez faila).
- Jeśli SES padnie: zwraca `WARN_EMAIL_FAILED` (bez faila).

---

## Kluczowe zbiory danych i miejsca zapisu

| Obiekt | Typ | Lokalizacja | Tworzony/odświeżany w kroku |
|---|---|---|---|
| `raw_archive` | Athena + S3 partycjonowane | `motobi_cepik_hist.raw_archive` + `s3://motointel-cepik-raw-prod/snapshots/archive/...` | 2 (pliki), 8 (metadane partycji) |
| `motobi_raw_latest` | Athena + S3 | `motobi_cepik.motobi_raw_latest` + `s3://motointel-cepik-raw-prod/snapshots/latest/` | 4 |
| `motobi_prod_latest` | Athena + S3 | `motobi_cepik.motobi_prod_latest` + `s3://motointel-cepik-raw-prod/prod-data/latest/` | 5 |
| `dim_brand` | Athena + S3 | `motobi_cepik.dim_brand` + `s3://motointel-cepik-raw-prod/dim/brand/` | 7 |
| `motobi_prod_snapshot_trend` | Athena | `motobi_cepik_hist.motobi_prod_snapshot_trend` | 9 |
| `motobi_top_brand_mom` (domyślnie) | DynamoDB | tabela DDB (`TOP_BRAND_MOM_DDB_TABLE`) | 10 |
| Raport XLSX | S3 | `s3://motointel-cepik-raw-prod/reports/report-<snapshot_date>.xlsx` | 2 (zapis), 11 (odczyt) |

---

## Wymagane zasoby / prerekwizyty AWS

- **Step Functions state machine** z definicją z `orchestrator.json`.
- **Lambda functions**:
  - `motobi-plan-snapshot`
  - `motobi-clear-raw-partitions`
  - `motobi-publish-latest-raw`
  - `motobi-qa-validate`
  - `motobi-refresh-dim-brand`
  - `motobi-build-top-brands-mom`
  - `motobi-notify`
- **ECS Fargate task definitions**:
  - worker CEPiK (`run-single-full-snapshot-worker`)
  - `motobi-build-prod-latest`
  - `motobi-repair-raw-archive`
  - `motobi-trend-builder`
- **Athena / Glue**:
  - bazy: `motobi_cepik`, `motobi_cepik_hist`
  - tabele źródłowe/wynikowe zgodne z powyższą sekcją
  - workgroup (rekomendacja: `motobi-etl` dla jobów z DML/CTAS)
- **S3**:
  - `motointel-cepik-raw-prod` (archive/latest/prod/dim/reports)
  - bucket wyników Athena (wg konfiguracji workgroup)
- **DynamoDB**:
  - tabela pod Top Brand MoM (domyślnie `motobi_top_brand_mom`, PK `snapshot_date`, SK `brand`)
- **SES**:
  - zweryfikowany sender i region skonfigurowany w env (`SES_REGION`, `EMAIL_SENDER`)

---

## Najważniejsze ENV dla newralgicznych kroków

### `motobi-build-top-brand-mom`
- `ATHENA_WORKGROUP` (default: `motobi-etl`)
- `ATHENA_FALLBACK_WORKGROUP` (default: `motobi-etl`)
- `TOP_BRAND_MOM_DDB_TABLE` (default: `motobi_top_brand_mom`)
- `ATHENA_POLL_INTERVAL_SEC`, `ATHENA_TIMEOUT_SEC`

### `motobi-notify`
- `REPORT_BUCKET`, `REPORT_PREFIX`
- `SES_REGION`, `EMAIL_SENDER`, `EMAIL_RECIPIENTS`

### `run-single-full-snapshot-worker`
- `SNAPSHOT_DATE`, `S3_BUCKET`, `S3_PREFIX`
- `YEARS`, `MONTH_START`, `MONTH_END`, `TYP_DATY`
- retry/timeouts i parametry API (np. `LIMIT`, `TIMEOUT`, `RETRIES`)

---

## Uwagi operacyjne

- Pipeline jest sekwencyjny i nie ma retry/catch zdefiniowanych na poziomie state machine; obsługa błędów jest głównie w kodzie poszczególnych kroków.
- Krok `Build Top Brand MoM` jest teraz niezależny od tabeli Athena `top_brand_mom_snapshot` — źródłem jest `raw_archive`, a wynik trafia do DynamoDB.
- `Publish RAW` zawsze robi full replace `snapshots/latest/` (delete + copy).

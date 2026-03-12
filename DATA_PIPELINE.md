# MotoIntel Data Pipeline (CEPiK)

## Podsumowanie

Pipeline Step Functions uruchamia sekwencyjny proces budowy snapshotu CEPiK:
1. planuje zakres,
2. pobiera pełny snapshot do archiwum S3,
3. waliduje kompletność snapshotu (gate),
4. publikuje `raw_latest`,
5. buduje warstwy analityczne (PROD, trend, dim),
6. liczy Top Brand MoM i zapisuje wynik do DynamoDB,
7. wysyła notyfikację z raportem XLSX.

Orkiestracja jest liniowa (bez gałęzi równoległych) i kończy się krokiem `Send Notification`.

---

## Definicja orchestratora (kolejność kroków)

Źródło: `data_download_orchestrator/orchestrator.json`.

1. **Plan Snapshot** (`motobi-plan-snapshot`, Lambda)
2. **Run Single Full Snapshot Worker** (ECS Fargate sync)
3. **Validate Snapshot Completeness** (`motobi-validate-snapshot-completeness`, Lambda)
4. **Clear RAW Partitions (Workaround)** (`motobi-clear-raw-partitions`, Lambda)
5. **Publish RAW** (`motobi-publish-latest-raw`, Lambda)
6. **Build PROD** (ECS Fargate sync, task `motobi-build-prod-latest`)
7. **QA Validate** (`motobi-qa-validate`, Lambda)
8. **Refresh DIM Brand** (`motobi-refresh-dim-brand`, Lambda)
9. **Repair RAW Archive** (ECS Fargate sync, task `motobi-repair-raw-archive`)
10. **Build Snapshot Trend** (ECS Fargate sync, task `motobi-trend-builder`)
11. **Build Top Brand MoM** (`motobi-build-top-brands-mom`, Lambda)
12. **Send Notification** (`motobi-notify`, Lambda, `End: true`)

---

## Szczegóły kroków (co czyta / co zapisuje)

### 1) Plan Snapshot — `orchestrator_elements/motobi-plan-snapshot.py`
- Buduje obiekt `plan` na bazie input eventu (`start_year`, `end_year`, `month_start`, `month_end`).
- Generuje `snapshot_date` (`%Y-%m-%d-%H%M`).
- Wylicza `partitions_to_clear` dla miesięcy bieżących/przyszłych (w ramach zakresu runu).
- Zwraca konfigurację pod ECS worker i S3 (`s3_bucket`, `s3_prefix`).

### 2) Run Single Full Snapshot Worker — `orchestrator_elements/run-single-full-snapshot-worker.py`
- Pobiera dane z API CEPiK strona po stronie (retry HTTP + sanity retry).
- Zapisuje parquety do archiwum S3 pod `snapshots/archive/snapshot_date=.../year=.../month=.../type=.../wojewodztwo=.../`.
- Generuje raport XLSX i zapisuje do `reports/report-<snapshot_date>.xlsx`.
- Generuje też gate file kompletności `reports/completeness-<snapshot_date>.json`.

### 3) Validate Snapshot Completeness — `orchestrator_elements/motobi-validate-snapshot-completeness.py`
- Odczytuje `reports/completeness-<snapshot_date>.json`.
- Przepuszcza pipeline tylko dla `status=COMPLETE` i `missed_pages_count=0`.
- Dopuszcza override tylko przy obecności `reports/completeness-approved-<snapshot_date>.json`.
- W przypadku niekompletności przerywa pipeline (hard fail).

### 4) Clear RAW Partitions — `orchestrator_elements/motobi-clear-raw-partitions.py`
- Opcjonalny cleanup wskazanych partycji snapshotu (na podstawie `partitions_to_clear`).
- Usuwa obiekty S3 pod prefiksami `snapshots/archive/snapshot_date=.../year=.../month=.../`.

### 5) Publish RAW — `orchestrator_elements/motobi-publish-latest-raw.py`
- Czyści `snapshots/latest/`.
- Kopiuje cały prefiks bieżącego snapshotu z `archive/snapshot_date=.../` do `latest/`.

### 6) Build PROD — `orchestrator_elements/motobi-build-prod-latest.py`
- Athena (`motobi_cepik`):
  - `MSCK REPAIR TABLE motobi_raw_latest`,
  - `DROP TABLE IF EXISTS motobi_prod_latest`,
  - CTAS `motobi_prod_latest` do `s3://motointel-cepik-raw-prod/prod-data/latest/`.
- Czyści też fizyczny prefix `prod-data/latest/` przed CTAS.
- Zawiera filtry biznesowe i agregację `COUNT(DISTINCT id)`.

### 7) QA Validate — `orchestrator_elements/motobi-qa-validate.py`
- Athena (`motobi_cepik`):
  - liczy `COUNT(*)` w `motobi_raw_latest`,
  - liczy `SUM(total_count)` w `motobi_prod_latest`.
- Zwraca metryki QA (`diff`, `ratio_prod_to_raw`), ale nie ma twardego progu failującego pipeline.

### 8) Refresh DIM Brand — `orchestrator_elements/motobi-refresh-dim-brand.py`
- Rebuild tabeli `dim_brand` z `motobi_raw_latest`.
- Kasuje i odtwarza tabelę external w `s3://motointel-cepik-raw-prod/dim/brand/`.

### 9) Repair RAW Archive — `orchestrator_elements/motobi-repair-raw-archive.py`
- Skanuje fizyczne partycje w S3 dla `snapshot_date`.
- Rejestruje brakujące partycje w Athena (`ALTER TABLE raw_archive ADD IF NOT EXISTS PARTITION ...`).

### 10) Build Snapshot Trend — `orchestrator_elements/motobi-build-snapshot-trend.py`
- Dla `snapshot_month = snapshot_date[:7]`:
  - sprawdza idempotencję w `motobi_prod_snapshot_trend`,
  - robi `INSERT INTO ... SELECT ...` z `raw_archive`.
- Dodaje trend tylko raz na miesiąc snapshotu.

### 11) Build Top Brand MoM — `orchestrator_elements/motobi-build-top-brand-mom.py`
- Liczy Top 20 marek wg logiki snapshot-vs-snapshot:
  - bieżąca wartość = liczba aut marki w **bieżącym snapshotcie** dla miesiąca rejestracji `snapshot_month - 1`,
  - wartość porównawcza = liczba aut marki w **poprzednim snapshotcie** dla miesiąca `snapshot_month - 2`.
- Wykorzystuje `raw_archive` jako źródło historyczne.
- Zapisuje wynik (upsert) do DynamoDB (`TOP_BRAND_MOM_DDB_TABLE`, domyślnie `motobi_top_brand_mom`) z kluczem:
  - `PK: snapshot_date`
  - `SK: brand`
- Obsługuje fallback workgroup Athena (`ATHENA_WORKGROUP` + `ATHENA_FALLBACK_WORKGROUP`) przy błędach Managed Results.

### 12) Send Notification — `orchestrator_elements/motobi-notify.py`
- Sprawdza raport XLSX w S3 (`reports/report-<snapshot_date>.xlsx`).
- Generuje pre-signed URL (7 dni).
- Wysyła mail przez SES z linkiem i załącznikiem.
- Jeśli raportu nie ma: zwraca `OK_NO_REPORT` (bez faila).
- Jeśli SES padnie: zwraca `WARN_EMAIL_FAILED` (bez faila).

---

## Kluczowe zbiory danych i miejsca zapisu

| Obiekt | Typ | Lokalizacja | Tworzony/odświeżany w kroku |
|---|---|---|---|
| `raw_archive` | Athena + S3 partycjonowane | `motobi_cepik_hist.raw_archive` + `s3://motointel-cepik-raw-prod/snapshots/archive/...` | 2 (pliki), 9 (metadane partycji) |
| `motobi_raw_latest` | Athena + S3 | `motobi_cepik.motobi_raw_latest` + `s3://motointel-cepik-raw-prod/snapshots/latest/` | 5 |
| `motobi_prod_latest` | Athena + S3 | `motobi_cepik.motobi_prod_latest` + `s3://motointel-cepik-raw-prod/prod-data/latest/` | 6 |
| `dim_brand` | Athena + S3 | `motobi_cepik.dim_brand` + `s3://motointel-cepik-raw-prod/dim/brand/` | 8 |
| `motobi_prod_snapshot_trend` | Athena | `motobi_cepik_hist.motobi_prod_snapshot_trend` | 10 |
| `motobi_top_brand_mom` (domyślnie) | DynamoDB | tabela DDB (`TOP_BRAND_MOM_DDB_TABLE`) | 11 |
| Raport XLSX | S3 | `s3://motointel-cepik-raw-prod/reports/report-<snapshot_date>.xlsx` | 2 (zapis), 12 (odczyt) |
| Completeness gate file | S3 JSON | `s3://motointel-cepik-raw-prod/reports/completeness-<snapshot_date>.json` | 2 (zapis), 3 (odczyt) |
| Manual override gate file | S3 JSON | `s3://motointel-cepik-raw-prod/reports/completeness-approved-<snapshot_date>.json` | 3 (opcjonalny odczyt) |

---

## Production readiness (ocena)

### Werdykt
**Warunkowo gotowy do pełnego runu produkcyjnego**: tak, jeśli zostaną spełnione bramki operacyjne poniżej.

### Co działa dobrze (must-have)
- Jest twardy gate kompletności snapshotu (`Validate Snapshot Completeness`) i fail przy brakach stron.
- Krytyczne warstwy `latest`, `prod-data/latest` i `dim/brand` są odświeżane atomowo przez full replace.
- Trend ma idempotencję na poziomie `snapshot_month`.
- Top Brand MoM ma upsert do DDB i fallback workgroup Athena.

### Co ogranicza „pełną” gotowość
- Brak globalnych `Retry/Catch` na poziomie Step Functions (awarie transientne kończą cały run bez automatycznej polityki retry).
- `QA Validate` nie posiada progów failujących run (to tylko metryka informacyjna).
- `motobi-publish-latest-raw` ma `base_prefix = "snapshots"` na sztywno (ignoruje `s3_prefix` z eventu), więc jest mniej elastyczny środowiskowo.
- Gate manual override opiera się o obecność pliku approval w S3; wymaga procedury operacyjnej, żeby uniknąć przypadkowego „przepchnięcia” niekompletnego snapshotu.

---

## Foldery S3 do czyszczenia przed pełnym runem produkcyjnym

### Wymagane czyszczenie ręczne (przed runem)
- **Brak twardo wymaganych folderów**: pipeline sam czyści kluczowe prefixy robocze (`snapshots/latest/`, `prod-data/latest/`, `dim/brand/`) we właściwych krokach.

### Zalecane czyszczenie ręczne (operacyjne hygiene)
- `reports/completeness-approved-<snapshot_date>.json` dla planowanego `snapshot_date` (upewnić się, że nie istnieje stary override).
- `reports/report-<snapshot_date>.xlsx` i `reports/completeness-<snapshot_date>.json` dla tego samego `snapshot_date`, jeśli robisz re-run z tym samym znacznikiem czasu.
- ewentualne ręcznie pozostawione artefakty testowe pod `reports/`.

### Prefixy czyszczone automatycznie przez pipeline
- `s3://motointel-cepik-raw-prod/snapshots/latest/` (krok Publish RAW)
- `s3://motointel-cepik-raw-prod/prod-data/latest/` (krok Build PROD)
- `s3://motointel-cepik-raw-prod/dim/brand/` (krok Refresh DIM Brand)
- wybrane partycje `s3://motointel-cepik-raw-prod/snapshots/archive/snapshot_date=.../year=.../month=.../` (krok Clear RAW Partitions)

---

## Mocne i słabe strony datalake

### Mocne strony
- Wyraźny podział na warstwy: `archive` (historyczny immutable-ish), `latest` (serving), `prod` (agregacja biznesowa), `dim` (wymiary).
- Bardzo dobra audytowalność dzięki snapshotowaniu i raportowi XLSX + completeness JSON.
- Silny komponent historyczny (`raw_archive` + `snapshot_trend`) pozwala na analizy as-of oraz porównania między snapshotami.
- Łatwość konsumpcji przez downstream: gotowy `prod_latest`, osobny `dim_brand`, DDB dla niskolatencyjnych KPI (Top Brand MoM).

### Słabe strony
- Brak jednej, centralnej warstwy jakości danych z twardymi SLA/progami (QA jest miękkie).
- Część logiki krytycznej oparta o „workaroundy” i ręczne operacje (manual approval file, clear partitions).
- Brak natywnej odporności orkiestracji na błędy transientne (retry/catch w state machine).
- Duża część ścieżek S3 i nazw zasobów jest zahardkodowana per środowisko produkcyjne (mniejsza przenośność/stageability).

---

## Wymagane zasoby / prerekwizyty AWS

- **Step Functions state machine** z definicją z `orchestrator.json`.
- **Lambda functions**:
  - `motobi-plan-snapshot`
  - `motobi-validate-snapshot-completeness`
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

- Pipeline jest sekwencyjny; brak retry/catch na poziomie state machine (warto dodać dla transient errors).
- Krok `Build Top Brand MoM` jest niezależny od tabeli Athena `top_brand_mom_snapshot` — źródłem jest `raw_archive`, a wynik trafia do DynamoDB.
- `Publish RAW` zawsze robi full replace `snapshots/latest/` (delete + copy).
- Dla pełnych runów zalecane jest uruchomienie w oknie czasowym o niskim ryzyku limitów API CEPiK.

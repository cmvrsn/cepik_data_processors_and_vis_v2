# MotoIntel Data Pipeline

## Podsumowanie

Pipeline pobiera dane CEPiK dla zadanego zakresu lat i miesięcy, zapisuje je do archiwum RAW w S3, a następnie publikuje aktualny snapshot jako „latest”. Na tej bazie budowane są tabele analityczne (PROD, trend, top brand MoM, dim_brand) oraz raport XLSX wysyłany w notyfikacji.

---

## Kroki pipeline (State Machine)

**Krok 1 — Plan Snapshot**  
Opis: Lambda przygotowuje parametry uruchomienia snapshotu (zakres lat, zakres miesięcy, `snapshot_date`, zasoby ECS/S3). Czyta event wejściowy uruchomienia i zapisuje plan przekazywany do kolejnych kroków.

**Krok 2 — Run Single Full Snapshot Worker**  
Opis: Worker pobiera dane z API CEPiK segmentami (typ pojazdu / województwo / rok / miesiąc) i zapisuje surowe paczki parquet do archiwum snapshotu w S3. Dodatkowo buduje raport przebiegu pobierania i zapisuje raport XLSX w S3.

**Krok 3 — Clear RAW Partitions (Workaround)**  
Opis: Lambda czyści wskazane partycje snapshotu w archiwum RAW, aby usunąć niepożądane lub błędne fragmenty danych. Czyta listę partycji do wyczyszczenia z inputu i usuwa obiekty z odpowiednich prefiksów S3.

**Krok 4 — Publish RAW**  
Opis: Lambda publikuje bieżący snapshot jako „latest” dla RAW, kopiując dane z `archive/snapshot_date=...` do `latest/`. Czyta dane RAW bieżącego snapshotu i zapisuje nową wersję „latest” w S3.

**Krok 5 — Build PROD**  
Opis: Lambda buduje tabelę analityczną `motobi_prod_latest` przez agregację i filtrowanie danych z `motobi_raw_latest`. Czyta tabelę RAW latest w Athena i zapisuje nową tabelę PROD wraz z plikami parquet w docelowym prefiksie S3.

**Krok 6 — QA Validate**  
Opis: Lambda wykonuje kontrolę jakości przez porównanie liczności RAW i sumy agregatów w PROD. Czyta `motobi_raw_latest` i `motobi_prod_latest`, a następnie zwraca metryki walidacyjne do logów/wyjścia kroku.

**Krok 7 — Refresh DIM Brand**  
Opis: Lambda odświeża wymiar marek/modeli (`dim_brand`) na podstawie najnowszego RAW. Czyta `motobi_raw_latest` i zapisuje zrekonstruowaną tabelę `dim_brand` w Athena.

**Krok 8 — Repair RAW Archive**  
Opis: Task ECS wykrywa fizycznie istniejące partycje snapshotu w `raw_archive` i rejestruje je w Athena (`ALTER TABLE ... ADD PARTITION`). Czyta strukturę folderów archiwum w S3 i aktualizuje metadane tabeli historycznej.

**Krok 9 — Build Snapshot Trend**  
Opis: Task ECS dopisuje dane nowego miesiąca snapshotu do tabeli trendowej `motobi_prod_snapshot_trend`, jeśli miesiąc nie był jeszcze przetworzony. Czyta `raw_archive` i zapisuje nowy wsad do tabeli trendowej w Athena.

**Krok 10 — Build Top Brand MoM**  
Opis: Lambda buduje miesięczne porównanie top marek (`top_brand_mom_snapshot`) względem poprzedniego snapshotu. Czyta `raw_archive` (bieżący i poprzedni snapshot) i zapisuje wynik do tabeli MoM.

**Krok 11 — Send Notification**  
Opis: Lambda sprawdza raport XLSX w S3, generuje link pre-signed i wysyła e-mail przez SES z linkiem oraz załącznikiem. Czyta plik raportu z S3 i nie zapisuje nowych danych analitycznych.

---

## Gdzie zapisywane są dane

| Dataset / tabela | Lokalizacja (S3 lub Athena) | Tworzona w kroku | Czy musi istnieć przed startem pipeline |
| ---------------- | --------------------------- | ---------------- | --------------------------------------- |
| raw_archive | Athena: `motobi_cepik_hist.raw_archive` + S3: `s3://motointel-cepik-raw-prod/snapshots/archive/snapshot_date=<...>/year=<...>/month=<...>/type=<...>/wojewodztwo=<...>/` | Krok 2 (pliki S3), Krok 8 (rejestracja partycji) | **TAK** (tabela Athena musi istnieć) |
| motobi_raw_latest | Athena: `motobi_cepik.motobi_raw_latest` + S3: `s3://motointel-cepik-raw-prod/snapshots/latest/` | Krok 4 (publikacja plików latest) | **TAK** (tabela Athena musi istnieć) |
| motobi_prod_latest | Athena: `motobi_cepik.motobi_prod_latest` + S3: `s3://motointel-cepik-raw-prod/prod-data/latest/` | Krok 5 | **NIE** |
| dim_brand | Athena: `motobi_cepik.dim_brand` | Krok 7 | **NIE** |
| motobi_prod_snapshot_trend | Athena: `motobi_cepik_hist.motobi_prod_snapshot_trend` | Krok 9 | **TAK** (krok wykonuje `INSERT`, bez tworzenia tabeli) |
| top_brand_mom_snapshot | Athena: `motobi_cepik_hist.top_brand_mom_snapshot` | Krok 10 | **NIE** (krok utworzy tabelę, jeśli jej nie ma) |
| raporty XLSX | S3: `s3://motointel-cepik-raw-prod/reports/report-<snapshot_date>.xlsx` | Krok 2 (zapis), Krok 11 (odczyt i wysyłka) | **NIE** |

---

## Cel dokumentu

Dokument służy jako szybka instrukcja operacyjna: pokazuje przepływ danych między krokami, ułatwia przygotowanie testowego uruchomienia i wskazuje, które tabele/metadane Athena trzeba przygotować przed startem pipeline.

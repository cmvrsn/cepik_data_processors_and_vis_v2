# CEPIK Data Download Orchestrator — opis elementów

Ten dokument opisuje, co robi każdy stan w state machine (`orchestrator.json`) i jakie zadania realizują skrypty z `orchestrator_elements/`.

## Kolejność stanów
1. Plan Snapshot
2. Run Single Full Snapshot Worker
3. Publish RAW
4. Build PROD
5. QA Validate
6. Refresh DIM Brand
7. Repair RAW Archive
8. Build Snapshot Trend
9. Send Notification

## Krótkie podsumowanie procesu
Pipeline buduje pełny snapshot RAW z CEPIK, publikuje go jako „latest”, tworzy tabelę PROD, wykonuje walidację QA, odświeża wymiar brand/model, rejestruje partycje archiwum historycznego, buduje trend miesięczny i na końcu wysyła notyfikację e-mail.

## Uzasadnienie nowego kroku `Refresh DIM Brand`
- Dashboard korzysta z tabeli `motobi_cepik.dim_brand` dla filtrów brand/model.
- Odświeżenie po `QA Validate` zapewnia spójność z już zbudowaną i zwalidowaną warstwą `latest`.
- Krok przebudowuje `dim_brand` z `motobi_raw_latest` jako `SELECT DISTINCT marka AS brand, model`.

## Double-check innych DIM
- `dim_region` jest używana jako słownik województw/powiatów i nie jest wyliczana dynamicznie z aktualnego snapshotu.
- `dim_vehicle_subtype` pełni rolę tabeli mapowania/kanonizacji (logika biznesowa), więc nie powinna być bezwarunkowo automatycznie nadpisywana z RAW.
- `dim_metadata` (origin/alt_fuel) może potencjalnie wymagać odświeżania tylko jeśli proces biznesowy zakłada pełną automatyzację słownika; obecnie wygląda na tabelę kontrolowaną.

Wniosek: automatyczne odświeżanie `dim_brand` jest krytyczne i zasadne; pozostałe DIM-y lepiej pozostawić jako managed/reference, chyba że zostanie podjęta decyzja o pełnej automatyzacji słowników.

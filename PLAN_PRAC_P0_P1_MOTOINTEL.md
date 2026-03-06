# MotoIntel CEPIK – plan prac (P0/P1)

Ten dokument jest bazą realizacji kolejnych zmian w repozytorium. Zawiera podział na zadania, zakres, kroki wykonawcze i kryteria akceptacji.

## Cel
- **P0:**
  1. Migracja budowy pełnej tabeli trendowej (archiwalne snapshoty, tabela łączona pod trendy) do ECS.
  2. Zaprojektowanie i wdrożenie systemu odświeżania tabel **DIM** (bez tabeli populacji).
- **P1:**
  1. Dokończenie/hardening warstwy wizualizacji.
  2. Przyspieszenie architektoniczne (mniej kosztownych zapytań i szybszy czas odpowiedzi).

---

## Zad. 1: Migracja budowy pełnej tabeli trendowej do ECS (P0)

### Zakres
Przenieść logikę budowy trendu historycznego z Lambdy do dedykowanego taska ECS uruchamianego ze Step Functions.

### Co trzeba zrobić
- Zdefiniować nowy task ECS, np. `motobi-trend-builder`.
- Wynieść obecną logikę SQL z funkcji trendowej do modułu uruchamialnego w kontenerze.
- W Step Functions podmienić krok trendowy na `ecs:runTask.sync`.
- Dodać timeouty techniczne i retry policy na poziomie kroku orchestracji.
- Uporządkować idempotencję:
  - minimum: bezpieczny rerun dla tego samego `snapshot_month`;
  - docelowo: build do wersji tymczasowej i promotion po walidacji.
- Ustalić kontrakt wejścia taska ECS:
  - `snapshot_date`,
  - `run_id` (zalecane),
  - target DB/table.

### Artefakty
- Definicja taska ECS (infra).
- Kod kontenera `trend-builder`.
- Zmiana ASL (Step Functions) – nowy krok ECS.
- Runbook operacyjny (jak wznowić, jak rollbackować).

### Kryteria akceptacji
- Ten sam wynik biznesowy co obecnie dla tego samego `snapshot_date`.
- Brak timeoutów Lambdy w kroku trendowym.
- Możliwy bezpieczny rerun bez duplikacji danych.

---

## Zad. 2: Strategia idempotencji i publikacji dla trendu (P0)

### Zakres
Doprecyzować sposób zapisu i publikacji trendu tak, aby uniknąć duplikacji i problemów przy niepełnych runach.

### Co trzeba zrobić
- Ustalić model zapisu:
  - wariant A: `INSERT` z kontrolą istnienia + reguły nadpisania,
  - wariant B (preferowany): wersjonowanie (`run_id`) + promotion widoku/tabeli logicznej.
- Dodać walidacje po buildzie:
  - liczność rekordów,
  - spójność miesiąca `snapshot_month`,
  - brak nienormalnych nulli na kluczowych polach.
- Określić zachowanie przy retry i przy częściowej awarii.

### Artefakty
- Specyfikacja idempotencji.
- Checklista walidacji post-build.
- Decyzja architektoniczna (ADR).

### Kryteria akceptacji
- Dwa uruchomienia dla tego samego wejścia nie psują danych.
- Nie ma sytuacji „pół-opublikowanej” tabeli trendowej.

---

## Zad. 3: System odświeżania DIM (bez population) (P0)

### Zakres
Zbudować pełny proces odświeżania DIM używanych przez dashboard/API, bez ruszania tabeli `population`.

### DIM objęte zakresem
- `dim_region`
- `dim_brand`
- `dim_metadata`
- `dim_vehicle_subtype`
- `dim_powiat_mapping`

> **Poza zakresem:** `population` (zostaje bez zmian).

### Co trzeba zrobić
- Zaprojektować schematy DIM (kolumny biznesowe + techniczne).
- Ustalić źródła danych i reguły transformacji dla każdego DIM.
- Dodać etap orchestracji `Build DIMs` po udanym buildzie fact/trendu.
- Wdrożyć walidacje jakości dla DIM:
  - unikalność,
  - kompletność,
  - zgodność z fact (coverage).
- Wdrożyć bezpieczny publish DIM:
  - staging -> validate -> promote.
- Ustalić harmonogram odświeżania:
  - po każdym pełnym runie danych (preferowane),
  - opcjonalny tryb on-demand.

### Artefakty
- Specyfikacja schematu każdego DIM.
- SQL/ETL do budowy DIM.
- Dokument walidacji jakości.
- Zmiany orchestracji.

### Kryteria akceptacji
- Wszystkie endpointy filtrów działają na świeżych DIM.
- DIM są spójne z aktualnym fact.
- Brak ręcznych, ad-hoc poprawek przy standardowym runie.

---

## Zad. 4: Kontrakt i governance dla `dim_vehicle_subtype` i `dim_powiat_mapping` (P0)

### Zakres
Ustalić formalne reguły utrzymania dwóch najbardziej wrażliwych DIM (mapowania i normalizacje nazw).

### Co trzeba zrobić
- Zdefiniować reguły kanonizacji nazw i ścieżkę dla nowych/nieznanych wartości.
- Dodać statusy jakości wpisów (np. `verified`, `pending`).
- Dodać raport „braki mapowań” po każdym odświeżeniu.
- Ustalić, które elementy są automatyczne, a które wymagają review biznesowego.

### Artefakty
- Policy dokumentujący mapowania.
- Raport jakości mapowań.
- Procedura review/akceptacji.

### Kryteria akceptacji
- Stabilne i powtarzalne mapowanie powiatów/podrodzajów.
- Brak „cichych” rozjazdów nazw między warstwami.

---

## Zad. 5: Dokończenie i hardening warstwy wizualizacji (P1)

### Zakres
Zweryfikować kompletność dashboardu i dopracować warstwę integracyjną pod produkcję.

### Co trzeba zrobić
- Sprawdzić pełne pokrycie case’ów filtrów i kombinacji.
- Ujednolicić sposób pobierania danych (preferencja: przez stabilny kontrakt API).
- Dodać kontrolowane fallbacki dla pustych danych i timeoutów.
- Ustalić stabilne TTL cache dla zapytań i DIM.

### Artefakty
- Lista scenariuszy testowych dashboardu.
- Matryca endpoint -> widget.
- Drobne poprawki UX/obsługi błędów.

### Kryteria akceptacji
- Dashboard działa poprawnie dla głównych scenariuszy biznesowych.
- Przewidywalne zachowanie przy brakach danych i opóźnieniach.

---

## Zad. 6: Przyspieszenie architektoniczne wizualizacji (P1)

### Zakres
Zmniejszyć koszt i opóźnienia zapytań analitycznych.

### Co trzeba zrobić
- Zidentyfikować najdroższe zapytania (top N).
- Zgrupować zapytania per ekran (ograniczyć model „1 widget = 1 query”).
- Rozważyć preagregacje pod najczęstsze przekroje (miesiąc/typ/region/paliwo).
- Dodać cache serwerowy po kluczu filtrów.
- Ustalić limity i timeouty endpointów, żeby nie blokowały UI.

### Artefakty
- Raport wydajności baseline vs po zmianach.
- Plan preagregacji (jeśli wdrażamy).
- Zmiany API pod batchowe odpowiedzi.

### Kryteria akceptacji
- Skrócenie czasu renderu krytycznych widoków.
- Mniejsza liczba zapytań i niższy koszt skanowania danych.

---

## Proponowana kolejność realizacji
1. Zad. 1 (ECS trend builder)
2. Zad. 2 (idempotencja/publish trendu)
3. Zad. 3 (framework odświeżania DIM)
4. Zad. 4 (governance mapowań)
5. Zad. 5 (hardening dashboardu)
6. Zad. 6 (optymalizacje wydajnościowe)

---

## Ryzyka projektowe (na teraz)
- Równoległe runy pipeline’u bez blokady wykonania.
- Niepełna idempotencja przy retry.
- Brak formalnej warstwy publish/promote dla DIM i trendu.
- Wysoki koszt zapytań ad-hoc przy rosnącym wolumenie.

---

## Definition of Done (dla tej fazy dokumentacyjnej)
- Jest wspólny, zaakceptowany plan P0/P1.
- Każde zadanie ma zakres, kroki, artefakty i kryteria akceptacji.
- Plan jest gotowy jako podstawa do implementacji kodu w kolejnych iteracjach.

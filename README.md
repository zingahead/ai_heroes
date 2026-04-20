# ai_heroes
AI-Programm der TUM

---

# Datawarehouse Asset Management (Oracle 19c PL/SQL)

Dieses Repository enthält vollständige Oracle 19c PL/SQL-Skripte für ein **Datawarehouse im Asset Management**. Die Architektur folgt einem klassischen 3-Layer ETL-Ansatz (Staging → Vortabellen → Zieltabellen) mit einem Sternschema für das DWH.

## Architekturüberblick

```
┌─────────────────────────────────────────────────────────────────────┐
│                         QUELLSYSTEME                                │
│  ┌──────────────────┐  ┌────────────────┐  ┌───────────────────┐  │
│  │  Fondsbuchhaltung │  │ Kursversorgung │  │ Wertpapier-       │  │
│  │  - Bestände       │  │ - Kurse/Preise │  │ Stammdaten        │  │
│  │  - Transaktionen  │  │ - Wechselkurse │  │ - WP-Stammdaten   │  │
│  │  - Fondsbewegungen│  │               │  │ - Fonds-Stammdaten│  │
│  └────────┬─────────┘  └───────┬────────┘  └────────┬──────────┘  │
└───────────┼────────────────────┼────────────────────┼─────────────┘
            │                   │                    │
            ▼                   ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│               LAYER 1: STAGING (1:1 Rohkopie)                       │
│  STG_FB_BESTAND        STG_KV_KURS          STG_WP_STAMMDATEN       │
│  STG_FB_TRANSAKTION    STG_KV_WECHSELKURS   STG_WP_FONDS_STAMMDATEN │
│  STG_FB_FONDSBEWEGUNG                       STG_LADELOG             │
└─────────────────────────────────────┬───────────────────────────────┘
                                      │ PKG_ETL_VORTABELLEN
                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│            LAYER 2: VORTABELLEN (Transformiert/Validiert)           │
│  VOR_WERTPAPIER    VOR_KURS          VOR_BESTAND                    │
│  VOR_FONDS         VOR_WECHSELKURS   VOR_TRANSAKTION                │
│                                      VOR_FONDSBEWEGUNG              │
└─────────────────────────────────────┬───────────────────────────────┘
                                      │ PKG_ETL_ZIELTABELLEN
                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│       LAYER 3: ZIELTABELLEN (DWH Sternschema)                       │
│                                                                     │
│  DIMENSIONSTABELLEN:          FAKTENTABELLEN:                       │
│  DIM_WERTPAPIER (SCD Typ 2)   FAKT_BESTAND                         │
│  DIM_FONDS      (SCD Typ 2)   FAKT_TRANSAKTION                     │
│  DIM_WAEHRUNG                 FAKT_FONDSBEWEGUNG                   │
│  DIM_LAND                     FAKT_KURS                            │
│  DIM_BRANCHE                  FAKT_WECHSELKURS                     │
│  DIM_DEPOT                                                          │
│  DIM_ZEIT                                                           │
└─────────────────────────────────────────────────────────────────────┘
```

## Dateistruktur

```
sql/
├── 01_staging/
│   └── 01_create_staging_tables.sql    # DDL: Staging-Tabellen (STG_*)
├── 02_vortabellen/
│   └── 01_create_vortabellen.sql       # DDL: Vortabellen (VOR_*)
├── 03_zieltabellen/
│   ├── 01_create_dimension_tables.sql  # DDL: Dimensionstabellen (DIM_*)
│   └── 02_create_fact_tables.sql       # DDL: Faktentabellen (FAKT_*)
├── 04_etl/
│   ├── 01_pkg_etl_staging.sql          # Package: Staging-Beladung
│   ├── 02_pkg_etl_vortabellen.sql      # Package: Transformation
│   ├── 03_pkg_etl_zieltabellen.sql     # Package: Zieltabellen-Beladung
│   └── 04_pkg_etl_control.sql          # Package: ETL-Orchestrierung
└── 05_jobs/
    └── 01_scheduler_jobs.sql           # Oracle Scheduler Jobs
install.sql                             # Master-Installationsskript
```

## Datenbankschema

### Staging-Tabellen (Layer 1: 1:1 Rohdatenkopie)

| Tabelle | Quellsystem | Inhalt |
|---|---|---|
| `STG_FB_BESTAND` | Fondsbuchhaltung | Tagesaktuelle Fondsbestände |
| `STG_FB_TRANSAKTION` | Fondsbuchhaltung | Wertpapiertransaktionen (Kauf/Verkauf) |
| `STG_FB_FONDSBEWEGUNG` | Fondsbuchhaltung | Mittelzu-/-abflüsse (Ausgabe/Rücknahme) |
| `STG_KV_KURS` | Kursversorgung | Marktpreise und Kurse |
| `STG_KV_WECHSELKURS` | Kursversorgung | Devisenkurse |
| `STG_WP_STAMMDATEN` | Wertpapierstammdaten | Wertpapier-Stammdaten |
| `STG_WP_FONDS_STAMMDATEN` | Wertpapierstammdaten | Fonds-Stammdaten |
| `STG_LADELOG` | DWH intern | Protokoll aller Staging-Ladeläufe |

### Vortabellen (Layer 2: Transformiert und validiert)

| Tabelle | Inhalt |
|---|---|
| `VOR_WERTPAPIER` | Bereinigte Wertpapier-Stammdaten |
| `VOR_FONDS` | Bereinigte Fonds-Stammdaten |
| `VOR_KURS` | Normalisierte Kursdaten (inkl. EUR-Kurs) |
| `VOR_WECHSELKURS` | Validierte Devisenkurse |
| `VOR_BESTAND` | Transformierte Bestände (inkl. EUR-Wert, Portfoliogewicht) |
| `VOR_TRANSAKTION` | Normalisierte Transaktionen |
| `VOR_FONDSBEWEGUNG` | Normalisierte Fondsbewegungen |

### Dimensionstabellen (Layer 3: Sternschema)

| Tabelle | SCD | Inhalt |
|---|---|---|
| `DIM_WERTPAPIER` | Typ 2 | Wertpapier-Stammdaten mit Historisierung |
| `DIM_FONDS` | Typ 2 | Fonds-Stammdaten mit Historisierung |
| `DIM_WAEHRUNG` | Typ 2 | Währungen (ISO 4217) |
| `DIM_LAND` | Typ 2 | Länder (ISO 3166-1) |
| `DIM_BRANCHE` | Typ 2 | Branchen und Sektoren (GICS) |
| `DIM_DEPOT` | Typ 2 | Depots |
| `DIM_ZEIT` | Statisch | Kalender-Zeitdimension (2000–2040) |

### Faktentabellen (Layer 3: Kennzahlen)

| Tabelle | Typ | Inhalt |
|---|---|---|
| `FAKT_BESTAND` | Snapshot | Tagesaktuelle Fondsbestände |
| `FAKT_TRANSAKTION` | Transaktional | Wertpapiertransaktionen |
| `FAKT_FONDSBEWEGUNG` | Transaktional | Ausgaben und Rücknahmen |
| `FAKT_KURS` | Snapshot | Marktpreise und Kurse |
| `FAKT_WECHSELKURS` | Snapshot | Devisenkurse |

## ETL-Packages

| Package | Verantwortlichkeit |
|---|---|
| `PKG_ETL_STAGING` | Extraktion aus Quellsystemen → Staging-Tabellen |
| `PKG_ETL_VORTABELLEN` | Transformation und Validierung → Vortabellen |
| `PKG_ETL_ZIELTABELLEN` | SCD Typ 2, MERGE → Dimensions- und Faktentabellen |
| `PKG_ETL_CONTROL` | Orchestrierung, Protokollierung, Monitoring |

## Installation

### Voraussetzungen
- Oracle Database 19c (oder höher)
- DWH-Schema-Benutzer mit CREATE TABLE, CREATE PROCEDURE, CREATE VIEW, CREATE SEQUENCE, CREATE JOB Rechten

### Schritt-für-Schritt

```sql
-- 1. Als DWH-Schema-Owner einloggen
-- 2. Gesamtinstallation ausführen
@install.sql
```

Oder schrittweise:

```sql
-- Layer 1: Staging
@sql/01_staging/01_create_staging_tables.sql

-- Layer 2: Vortabellen
@sql/02_vortabellen/01_create_vortabellen.sql

-- Layer 3: Dimensionstabellen
@sql/03_zieltabellen/01_create_dimension_tables.sql

-- Layer 3: Faktentabellen
@sql/03_zieltabellen/02_create_fact_tables.sql

-- ETL-Packages
@sql/04_etl/01_pkg_etl_staging.sql
@sql/04_etl/02_pkg_etl_vortabellen.sql
@sql/04_etl/03_pkg_etl_zieltabellen.sql
@sql/04_etl/04_pkg_etl_control.sql

-- Zeitdimension befüllen (einmalig)
BEGIN
    PROC_FILL_DIM_ZEIT(DATE '2000-01-01', DATE '2040-12-31');
END;
/

-- Scheduler-Jobs einrichten
@sql/05_jobs/01_scheduler_jobs.sql
```

## Betrieb

### Manueller ETL-Lauf (Test)

```sql
SET SERVEROUTPUT ON
EXEC PROC_MANUELLER_ETL_LAUF(TRUNC(SYSDATE));
```

### Einzelne ETL-Phasen ausführen

```sql
DECLARE
    v_batch_id NUMBER;
BEGIN
    -- Phase 1: Nur Staging
    PKG_ETL_CONTROL.run_staging_only(
        p_lade_datum => TRUNC(SYSDATE),
        p_batch_id   => v_batch_id
    );
    DBMS_OUTPUT.PUT_LINE('Staging Batch-ID: ' || v_batch_id);

    -- Phase 2: Nur Transformation (für bestehende Batch-ID)
    PKG_ETL_CONTROL.run_transform_only(v_batch_id);

    -- Phase 3: Nur Zieltabellen-Beladung
    PKG_ETL_CONTROL.run_zielbeladung_only(v_batch_id);
END;
/
```

### Scheduler-Jobs aktivieren (nach Tests)

```sql
BEGIN
    DBMS_SCHEDULER.ENABLE('JOB_DWH_TAEGLICH_ETL');
    DBMS_SCHEDULER.ENABLE('JOB_DWH_STAGING_CLEANUP');
END;
/
```

### Monitoring

```sql
-- ETL-Prozessprotokoll
SELECT * FROM V_ETL_PROZESSLOG_AKTUELL;

-- Staging-Ladeprotokoll
SELECT * FROM V_ETL_STAGING_LOG_AKTUELL;

-- Scheduler-Job-Status
SELECT * FROM V_ETL_JOB_STATUS;

-- Datenqualitätsbericht für letzten Batch
BEGIN PKG_ETL_CONTROL.check_data_quality(<batch_id>); END;
```

### Ungültige Datensätze prüfen

```sql
-- Ungültige Wertpapiere nach Transformation prüfen
SELECT isin, validierungsfehler
FROM   VOR_WERTPAPIER
WHERE  batch_id = <batch_id>
AND    ist_gueltig = 'N';

-- Ungültige Bestände
SELECT fonds_id, isin, bewertungsdatum, validierungsfehler
FROM   VOR_BESTAND
WHERE  batch_id = <batch_id>
AND    ist_gueltig = 'N';
```

## Quellsystem-Anbindung (Produktivbetrieb)

Im produktiven Einsatz werden die Testdaten-Simulationen in `PKG_ETL_STAGING`
durch echte Abfragen über Oracle Database Links ersetzt:

```sql
-- Beispiel: Bestände aus Fondsbuchhaltungs-DB-Link laden
INSERT INTO STG_FB_BESTAND (...)
SELECT p_batch_id, fonds_id, isin, bewertungsdatum, ...
FROM   BESTAND_VIEW@FONDSBUCH_DBLINK
WHERE  bewertungsdatum = p_lade_datum;
```

Unterstützte Quellsystem-Anbindungen:
- **Oracle DB-Links** (`@<DBLINK_NAME>`) für Oracle-Quellsysteme
- **Heterogene Dienste (HSODBC/DG4ODBC)** für Nicht-Oracle-Quellsysteme
- **External Tables** für dateibasierte Lieferungen (CSV/FTP)

## Design-Entscheidungen

| Thema | Entscheidung | Begründung |
|---|---|---|
| SCD-Typ | Typ 2 für DIM_WERTPAPIER, DIM_FONDS | Historisierung von Stammdatenänderungen |
| Batch-ID | Eindeutig pro Ladelauf (Sequenz) | Vollständige Nachvollziehbarkeit |
| EUR-Normalisierung | In Vortabellen berechnet | Einheitliche Reporting-Währung |
| Validierung | In Vortabellen, nicht in Staging | Rohdaten unverändert erhalten |
| Idempotenz | DELETE + INSERT in Vortabellen | Wiederholbarkeit ohne Duplikate |
| Duplikatschutz | MERGE in Faktentabellen | Sichere Mehrfachausführung |
| Zeitdimension | Vorberechnet (2000–2040) | Performance bei Joins |

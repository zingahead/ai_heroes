-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- INSTALLATIONSSKRIPT (Master-Install)
-- =============================================================================
-- Ausfuehren als DWH-Schema-Owner in SQL*Plus oder SQLcl:
--   @install.sql
-- =============================================================================

PROMPT =====================================================
PROMPT  DWH Asset Management - Installation gestartet
PROMPT =====================================================

-- Verbindungseinstellungen
SET ECHO ON
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000
SET TERMOUT ON

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

-- Schritt 1: Staging-Tabellen
PROMPT --- Schritt 1: Staging-Tabellen erstellen ---
@@sql/01_staging/01_create_staging_tables.sql

-- Schritt 2: Vortabellen
PROMPT --- Schritt 2: Vortabellen erstellen ---
@@sql/02_vortabellen/01_create_vortabellen.sql

-- Schritt 3: Zieltabellen (Dimensionen)
PROMPT --- Schritt 3a: Dimensionstabellen erstellen ---
@@sql/03_zieltabellen/01_create_dimension_tables.sql

-- Schritt 4: Zieltabellen (Fakten)
PROMPT --- Schritt 3b: Faktentabellen erstellen ---
@@sql/03_zieltabellen/02_create_fact_tables.sql

-- Schritt 5: ETL-Packages
PROMPT --- Schritt 4a: ETL Staging-Package ---
@@sql/04_etl/01_pkg_etl_staging.sql

PROMPT --- Schritt 4b: ETL Vortabellen-Package ---
@@sql/04_etl/02_pkg_etl_vortabellen.sql

PROMPT --- Schritt 4c: ETL Zieltabellen-Package ---
@@sql/04_etl/03_pkg_etl_zieltabellen.sql

PROMPT --- Schritt 4d: ETL Control-Package ---
@@sql/04_etl/04_pkg_etl_control.sql

-- Schritt 6: Zeitdimension befuellen (2000-2040)
PROMPT --- Schritt 5: Zeitdimension befuellen ---
BEGIN
    PROC_FILL_DIM_ZEIT(
        p_von_datum => DATE '2000-01-01',
        p_bis_datum => DATE '2040-12-31'
    );
    DBMS_OUTPUT.PUT_LINE('Zeitdimension befuellt.');
END;
/

-- Schritt 7: Scheduler-Jobs einrichten
PROMPT --- Schritt 6: Scheduler-Jobs einrichten ---
@@sql/05_jobs/01_scheduler_jobs.sql

PROMPT =====================================================
PROMPT  Installation abgeschlossen!
PROMPT
PROMPT  Naechste Schritte:
PROMPT  1. Ersten ETL-Testlauf ausfuehren:
PROMPT     EXEC PROC_MANUELLER_ETL_LAUF(TRUNC(SYSDATE));
PROMPT
PROMPT  2. Scheduler-Jobs aktivieren:
PROMPT     BEGIN DBMS_SCHEDULER.ENABLE('JOB_DWH_TAEGLICH_ETL'); END;
PROMPT =====================================================

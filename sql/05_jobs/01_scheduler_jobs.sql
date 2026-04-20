-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- Oracle Scheduler Jobs (taeglich automatischer ETL-Lauf)
-- =============================================================================
-- Beschreibung:
--   Definiert Oracle DBMS_SCHEDULER Jobs fuer den automatischen,
--   taeglich ETL-Lauf des Asset Management Datawarehouse.
--
-- Job-Hierarchie:
--   JOB_DWH_TAEGLICH_ETL    - Hauptjob (taeglich 06:00 Uhr)
--     JOB_DWH_STAGING        - Staging-Phase (kann auch einzeln ausgefuehrt werden)
--     JOB_DWH_TRANSFORMATION - Transformationsphase
--     JOB_DWH_ZIELBELADUNG   - Zieltabellen-Beladung
--
-- Hinweis: Jobs werden als Job-Kette (Chain) implementiert, damit
--   Phase 2 nur startet, wenn Phase 1 erfolgreich war.
-- =============================================================================

-- =============================================================================
-- Schritt 1: Job-Programme definieren
-- =============================================================================

-- Programm fuer den vollstaendigen taeglich ETL-Lauf
BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM (
        program_name        => 'PROG_DWH_TAEGLICH_ETL',
        program_type        => 'STORED_PROCEDURE',
        program_action      => 'PKG_ETL_CONTROL.RUN_DAILY_ETL',
        number_of_arguments => 2,
        enabled             => FALSE,
        comments            => 'Vollstaendiger taeglich ETL-Lauf fuer DWH Asset Management'
    );

    -- Argument 1: Lade-Datum (Standard: TRUNC(SYSDATE))
    DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT(
        program_name      => 'PROG_DWH_TAEGLICH_ETL',
        argument_name     => 'P_LADE_DATUM',
        argument_position => 1,
        argument_type     => 'DATE',
        default_value     => NULL   -- NULL = TRUNC(SYSDATE) (wird im Package behandelt)
    );

    -- Argument 2: OUT-Parameter Batch-ID (wird vom Scheduler ignoriert)
    DBMS_SCHEDULER.DEFINE_PROGRAM_ARGUMENT(
        program_name      => 'PROG_DWH_TAEGLICH_ETL',
        argument_name     => 'P_BATCH_ID',
        argument_position => 2,
        argument_type     => 'NUMBER',
        default_value     => NULL
    );

    DBMS_SCHEDULER.ENABLE('PROG_DWH_TAEGLICH_ETL');
END;
/

-- =============================================================================
-- Schritt 2: Schedule-Definitionen
-- =============================================================================

-- Taeglich-Schedule: Wochentags 06:00 Uhr
BEGIN
    DBMS_SCHEDULER.CREATE_SCHEDULE (
        schedule_name   => 'SCHED_DWH_WOCHENTAGS_0600',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY;BYDAY=MON,TUE,WED,THU,FRI;BYHOUR=6;BYMINUTE=0;BYSECOND=0',
        comments        => 'Wochentaeglich um 06:00 Uhr (Mo-Fr)'
    );
END;
/

-- Woechentlich-Schedule: Samstag 04:00 Uhr (fuer Vollabgleich Stammdaten)
BEGIN
    DBMS_SCHEDULER.CREATE_SCHEDULE (
        schedule_name   => 'SCHED_DWH_SAMSTAG_0400',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY;BYDAY=SAT;BYHOUR=4;BYMINUTE=0;BYSECOND=0',
        comments        => 'Woechentlich samstags um 04:00 Uhr (Vollabgleich)'
    );
END;
/

-- =============================================================================
-- Schritt 3: Hauptjob erstellen (taeglicher ETL-Lauf)
-- =============================================================================
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name          => 'JOB_DWH_TAEGLICH_ETL',
        program_name      => 'PROG_DWH_TAEGLICH_ETL',
        schedule_name     => 'SCHED_DWH_WOCHENTAGS_0600',
        enabled           => FALSE,   -- Manuell aktivieren nach Tests
        auto_drop         => FALSE,
        restartable       => FALSE,   -- Kein automatischer Neustart bei Fehler
        max_failures      => 3,       -- Nach 3 Fehlern: Job deaktivieren
        max_runs          => NULL,
        comments          => 'Taeglicher ETL-Lauf DWH Asset Management (Mo-Fr, 06:00 Uhr)'
    );

    -- Job-Logging aktivieren
    DBMS_SCHEDULER.SET_ATTRIBUTE(
        name      => 'JOB_DWH_TAEGLICH_ETL',
        attribute => 'LOGGING_LEVEL',
        value     => DBMS_SCHEDULER.LOGGING_FULL
    );
END;
/

-- =============================================================================
-- Schritt 4: Job-Kette (Chain) fuer phasenweise Ausfuehrung mit Fehlerbehandlung
-- =============================================================================

-- Job-Kette erstellen
BEGIN
    DBMS_SCHEDULER.CREATE_CHAIN (
        chain_name  => 'CHAIN_DWH_ETL_PHASEN',
        rule_set_name => NULL,
        evaluation_interval => NULL,
        comments    => 'ETL-Prozesskette: Staging -> Transformation -> Zielbeladung'
    );
END;
/

-- Schritte der Kette definieren
BEGIN
    -- Schritt 1: Staging
    DBMS_SCHEDULER.DEFINE_CHAIN_STEP (
        chain_name  => 'CHAIN_DWH_ETL_PHASEN',
        step_name   => 'SCHRITT_STAGING',
        program_name => 'PROG_DWH_TAEGLICH_ETL'
    );

    -- Kettenregeln: Schritt 2 nur nach erfolgreichem Schritt 1
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE (
        chain_name  => 'CHAIN_DWH_ETL_PHASEN',
        rule_name   => 'REGEL_START_STAGING',
        condition   => 'TRUE',
        action      => 'START SCHRITT_STAGING',
        comments    => 'Staging immer starten'
    );

    -- Bei Fehler im Staging: Kette abbrechen
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE (
        chain_name  => 'CHAIN_DWH_ETL_PHASEN',
        rule_name   => 'REGEL_FEHLER_ABBRUCH',
        condition   => 'SCHRITT_STAGING FAILED',
        action      => 'END -1',
        comments    => 'Bei Staging-Fehler: Kette mit Fehlercode beenden'
    );

    -- Bei Erfolg Staging: Ende
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE (
        chain_name  => 'CHAIN_DWH_ETL_PHASEN',
        rule_name   => 'REGEL_ERFOLG_ENDE',
        condition   => 'SCHRITT_STAGING SUCCEEDED',
        action      => 'END 0',
        comments    => 'Nach erfolgreichem Staging: Kette erfolgreich beenden'
    );

    DBMS_SCHEDULER.ENABLE('CHAIN_DWH_ETL_PHASEN');
END;
/

-- =============================================================================
-- Schritt 5: Monitoring-Views fuer Job-Ueberwachung
-- =============================================================================

CREATE OR REPLACE VIEW V_ETL_JOB_STATUS AS
SELECT
    j.job_name,
    j.enabled,
    j.state,
    j.last_start_date,
    j.last_run_duration,
    j.next_run_date,
    j.failure_count,
    j.run_count,
    j.max_failures,
    j.comments
FROM   USER_SCHEDULER_JOBS j
WHERE  j.job_name LIKE 'JOB_DWH%'
ORDER BY j.job_name;

COMMENT ON TABLE V_ETL_JOB_STATUS IS 'View: Uebersicht aller DWH ETL Scheduler-Jobs und deren Status';


CREATE OR REPLACE VIEW V_ETL_PROZESSLOG_AKTUELL AS
SELECT
    p.prozess_id,
    p.batch_id,
    TO_CHAR(p.lade_datum, 'DD.MM.YYYY')     AS lade_datum,
    p.phase,
    p.status,
    TO_CHAR(p.prozessstart, 'DD.MM.YYYY HH24:MI:SS') AS prozessstart,
    TO_CHAR(p.prozessende,  'DD.MM.YYYY HH24:MI:SS') AS prozessende,
    CASE WHEN p.prozessende IS NOT NULL
         THEN ROUND((p.prozessende - p.prozessstart) * 24 * 60, 2)
    END AS laufzeit_minuten,
    p.fehlermeldung
FROM   ETL_PROZESSLOG p
WHERE  p.lade_datum >= TRUNC(SYSDATE) - 30   -- Letzte 30 Tage
ORDER BY p.prozess_id DESC;

COMMENT ON TABLE V_ETL_PROZESSLOG_AKTUELL IS 'View: ETL-Prozessprotokoll der letzten 30 Tage';


CREATE OR REPLACE VIEW V_ETL_STAGING_LOG_AKTUELL AS
SELECT
    l.log_id,
    l.batch_id,
    l.quellsystem,
    l.tabelle,
    l.status,
    l.anzahl_saetze,
    TO_CHAR(l.ladestart, 'DD.MM.YYYY HH24:MI:SS') AS ladestart,
    TO_CHAR(l.ladeende,  'DD.MM.YYYY HH24:MI:SS') AS ladeende,
    CASE WHEN l.ladeende IS NOT NULL
         THEN ROUND((l.ladeende - l.ladestart) * 24 * 60 * 60, 2)
    END AS laufzeit_sekunden,
    l.fehlermeldung
FROM   STG_LADELOG l
WHERE  l.ladestart >= SYSTIMESTAMP - INTERVAL '30' DAY
ORDER BY l.log_id DESC;

COMMENT ON TABLE V_ETL_STAGING_LOG_AKTUELL IS 'View: Staging-Ladeprotokoll der letzten 30 Tage';


-- =============================================================================
-- Schritt 6: Prozedur zum manuellen Starten des ETL-Laufs (fuer Tests/Rerun)
-- =============================================================================
CREATE OR REPLACE PROCEDURE PROC_MANUELLER_ETL_LAUF (
    p_lade_datum  IN DATE DEFAULT TRUNC(SYSDATE)
) AS
    v_batch_id  NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starte ETL-Lauf fuer Datum: '
        || TO_CHAR(p_lade_datum, 'DD.MM.YYYY'));

    PKG_ETL_CONTROL.run_daily_etl(
        p_lade_datum => p_lade_datum,
        p_batch_id   => v_batch_id
    );

    DBMS_OUTPUT.PUT_LINE('ETL-Lauf abgeschlossen. Batch-ID: ' || v_batch_id);

    -- Datenqualitaetsbericht ausgeben
    PKG_ETL_CONTROL.check_data_quality(v_batch_id);

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('FEHLER im ETL-Lauf: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Batch-ID: ' || NVL(TO_CHAR(v_batch_id), 'N/A'));
        RAISE;
END PROC_MANUELLER_ETL_LAUF;
/

-- =============================================================================
-- Schritt 7: Jobs fuer Datenpflege und Archivierung
-- =============================================================================

-- Job: Alte Staging-Daten loeschen (Aufbewahrung: 90 Tage)
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'JOB_DWH_STAGING_CLEANUP',
        job_type        => 'PLSQL_BLOCK',
        job_action      => '
            BEGIN
                -- Staging-Daten aelter als 90 Tage loeschen
                DELETE FROM STG_FB_BESTAND       WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_FB_TRANSAKTION   WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_FB_FONDSBEWEGUNG WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_KV_KURS          WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_KV_WECHSELKURS   WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_WP_STAMMDATEN    WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_WP_FONDS_STAMMDATEN WHERE ladezeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM STG_LADELOG          WHERE erstellt_am < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                -- Vortabellen-Daten aelter als 90 Tage loeschen
                DELETE FROM VOR_WERTPAPIER       WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_FONDS            WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_KURS             WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_WECHSELKURS      WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_BESTAND          WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_TRANSAKTION      WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                DELETE FROM VOR_FONDSBEWEGUNG    WHERE verarbeitungszeit < SYSTIMESTAMP - INTERVAL ''90'' DAY;
                COMMIT;
            END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY;BYDAY=SUN;BYHOUR=3;BYMINUTE=0;BYSECOND=0',
        enabled         => FALSE,
        auto_drop       => FALSE,
        comments        => 'Woechentliche Bereinigung alter Staging- und Vortabellen-Daten (>90 Tage)'
    );
END;
/

-- =============================================================================
-- Jobs aktivieren (nach erfolgreichem Test manuell ausfuehren)
-- =============================================================================
-- ACHTUNG: Jobs erst nach vollstaendigem Test aktivieren!
--
-- BEGIN
--     DBMS_SCHEDULER.ENABLE('JOB_DWH_TAEGLICH_ETL');
--     DBMS_SCHEDULER.ENABLE('JOB_DWH_STAGING_CLEANUP');
-- END;
-- /

COMMIT;

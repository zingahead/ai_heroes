-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- ETL Steuerungspaket (PKG_ETL_CONTROL)
-- =============================================================================
-- Beschreibung:
--   Dieses Package orchestriert den gesamten ETL-Prozess und stellt die
--   zentrale Steuerung aller drei ETL-Phasen bereit:
--     Phase 1: Staging   (PKG_ETL_STAGING)
--     Phase 2: Transform (PKG_ETL_VORTABELLEN)
--     Phase 3: Load      (PKG_ETL_ZIELTABELLEN)
--
--   Ausserdem enthaelt es:
--   - Prozessprotokollierung
--   - Fehlerbehandlung und -benachrichtigung
--   - Monitoring-Funktionen
-- =============================================================================

-- Prozesstabelle fuer den ETL-Gesamtlauf
CREATE TABLE ETL_PROZESSLOG (
    prozess_id          NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id            NUMBER          NOT NULL,
    lade_datum          DATE            NOT NULL,
    prozessstart        TIMESTAMP       NOT NULL,
    prozessende         TIMESTAMP,
    phase               VARCHAR2(30)    NOT NULL,
                            -- STAGING, TRANSFORMATION, ZIELBELADUNG, GESAMT
    status              VARCHAR2(20)    DEFAULT 'GESTARTET'
                            CHECK (status IN ('GESTARTET','ERFOLGREICH','FEHLER','UEBERSPRUNGEN')),
    fehlermeldung       VARCHAR2(4000),
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP
);

COMMENT ON TABLE  ETL_PROZESSLOG        IS 'Protokoll aller ETL-Gesamtlaeufe und Phasen';
COMMENT ON COLUMN ETL_PROZESSLOG.phase  IS 'ETL-Phase: STAGING, TRANSFORMATION, ZIELBELADUNG, GESAMT';


CREATE OR REPLACE PACKAGE PKG_ETL_CONTROL AS

    -- =========================================================================
    -- Oeffentliche Typen
    -- =========================================================================
    TYPE t_etl_status IS RECORD (
        batch_id          NUMBER,
        lade_datum        DATE,
        status_staging    VARCHAR2(20),
        status_transform  VARCHAR2(20),
        status_zielladen  VARCHAR2(20),
        status_gesamt     VARCHAR2(20),
        fehler_detail     VARCHAR2(4000)
    );

    -- =========================================================================
    -- Hauptprozeduren
    -- =========================================================================

    -- Vollstaendigen taeglich ETL-Lauf starten (alle 3 Phasen)
    PROCEDURE run_daily_etl (
        p_lade_datum    IN  DATE    DEFAULT TRUNC(SYSDATE),
        p_batch_id      OUT NUMBER
    );

    -- Einzelne ETL-Phasen gezielt ausfuehren (z.B. fuer Reprocessing)
    PROCEDURE run_staging_only (
        p_lade_datum    IN  DATE    DEFAULT TRUNC(SYSDATE),
        p_batch_id      OUT NUMBER
    );

    PROCEDURE run_transform_only (
        p_batch_id      IN  NUMBER
    );

    PROCEDURE run_zielbeladung_only (
        p_batch_id      IN  NUMBER
    );

    -- Monitoring
    FUNCTION get_last_batch_status RETURN t_etl_status;

    PROCEDURE check_data_quality (
        p_batch_id  IN  NUMBER
    );

END PKG_ETL_CONTROL;
/


CREATE OR REPLACE PACKAGE BODY PKG_ETL_CONTROL AS

    -- =========================================================================
    -- Private Hilfsfunktionen
    -- =========================================================================

    PROCEDURE log_prozess_start (
        p_batch_id    IN NUMBER,
        p_lade_datum  IN DATE,
        p_phase       IN VARCHAR2,
        p_prozess_id  OUT NUMBER
    ) AS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO ETL_PROZESSLOG (batch_id, lade_datum, prozessstart, phase, status)
        VALUES (p_batch_id, p_lade_datum, SYSTIMESTAMP, p_phase, 'GESTARTET')
        RETURNING prozess_id INTO p_prozess_id;
        COMMIT;
    END log_prozess_start;

    PROCEDURE log_prozess_ende (
        p_prozess_id    IN NUMBER,
        p_status        IN VARCHAR2,
        p_fehlermeldung IN VARCHAR2 DEFAULT NULL
    ) AS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE ETL_PROZESSLOG
        SET    prozessende   = SYSTIMESTAMP,
               status        = p_status,
               fehlermeldung = SUBSTR(p_fehlermeldung, 1, 4000)
        WHERE  prozess_id    = p_prozess_id;
        COMMIT;
    END log_prozess_ende;


    -- =========================================================================
    -- RUN_DAILY_ETL
    -- Vollstaendiger taeglicher ETL-Lauf (alle 3 Phasen sequenziell)
    -- =========================================================================
    PROCEDURE run_daily_etl (
        p_lade_datum IN  DATE DEFAULT TRUNC(SYSDATE),
        p_batch_id   OUT NUMBER
    ) AS
        v_batch_id        NUMBER;
        v_prozess_id      NUMBER;
        v_fehler          VARCHAR2(4000);
    BEGIN
        -- Gesamt-Prozesslog starten
        -- Batch-ID wird in Phase 1 (Staging) vergeben
        v_batch_id := PKG_ETL_STAGING.get_new_batch_id();
        p_batch_id := v_batch_id;

        log_prozess_start(v_batch_id, p_lade_datum, 'GESAMT', v_prozess_id);

        BEGIN
            -- ===========================
            -- Phase 1: Staging
            -- ===========================
            DECLARE
                v_stg_prozess_id NUMBER;
            BEGIN
                log_prozess_start(v_batch_id, p_lade_datum, 'STAGING', v_stg_prozess_id);

                -- Wertpapier-Stammdaten (keine Datumsabhängigkeit)
                PKG_ETL_STAGING.load_wertpapierstammdaten(v_batch_id);
                -- Kursdaten
                PKG_ETL_STAGING.load_kursversorgung(v_batch_id, p_lade_datum);
                -- Fondsbuchhaltungs-Daten
                PKG_ETL_STAGING.load_fondsbuchhaltung(v_batch_id, p_lade_datum);

                log_prozess_ende(v_stg_prozess_id, 'ERFOLGREICH');
            EXCEPTION
                WHEN OTHERS THEN
                    v_fehler := SQLERRM;
                    log_prozess_ende(v_stg_prozess_id, 'FEHLER', v_fehler);
                    RAISE;
            END;

            -- ===========================
            -- Phase 2: Transformation
            -- ===========================
            DECLARE
                v_trf_prozess_id NUMBER;
            BEGIN
                log_prozess_start(v_batch_id, p_lade_datum, 'TRANSFORMATION', v_trf_prozess_id);

                PKG_ETL_VORTABELLEN.run_transformation(v_batch_id);

                log_prozess_ende(v_trf_prozess_id, 'ERFOLGREICH');
            EXCEPTION
                WHEN OTHERS THEN
                    v_fehler := SQLERRM;
                    log_prozess_ende(v_trf_prozess_id, 'FEHLER', v_fehler);
                    RAISE;
            END;

            -- ===========================
            -- Phase 3: Zieltabellen laden
            -- ===========================
            DECLARE
                v_ziel_prozess_id NUMBER;
            BEGIN
                log_prozess_start(v_batch_id, p_lade_datum, 'ZIELBELADUNG', v_ziel_prozess_id);

                PKG_ETL_ZIELTABELLEN.run_zielbeladung(v_batch_id);

                log_prozess_ende(v_ziel_prozess_id, 'ERFOLGREICH');
            EXCEPTION
                WHEN OTHERS THEN
                    v_fehler := SQLERRM;
                    log_prozess_ende(v_ziel_prozess_id, 'FEHLER', v_fehler);
                    RAISE;
            END;

            log_prozess_ende(v_prozess_id, 'ERFOLGREICH');

        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_prozess_ende(v_prozess_id, 'FEHLER', v_fehler);
                RAISE;
        END;

    EXCEPTION
        WHEN OTHERS THEN
            p_batch_id := NVL(v_batch_id, -1);
            RAISE;
    END run_daily_etl;


    -- =========================================================================
    -- RUN_STAGING_ONLY (Phase 1 einzeln)
    -- =========================================================================
    PROCEDURE run_staging_only (
        p_lade_datum IN  DATE DEFAULT TRUNC(SYSDATE),
        p_batch_id   OUT NUMBER
    ) AS
        v_batch_id        NUMBER;
        v_prozess_id      NUMBER;
        v_fehler          VARCHAR2(4000);
    BEGIN
        v_batch_id := PKG_ETL_STAGING.get_new_batch_id();
        p_batch_id := v_batch_id;

        log_prozess_start(v_batch_id, p_lade_datum, 'STAGING', v_prozess_id);
        BEGIN
            PKG_ETL_STAGING.run_full_staging(v_batch_id, p_lade_datum);
            log_prozess_ende(v_prozess_id, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_prozess_ende(v_prozess_id, 'FEHLER', v_fehler);
                RAISE;
        END;
    END run_staging_only;


    -- =========================================================================
    -- RUN_TRANSFORM_ONLY (Phase 2 einzeln, z.B. fuer Reprocessing)
    -- =========================================================================
    PROCEDURE run_transform_only (p_batch_id IN NUMBER) AS
        v_prozess_id  NUMBER;
        v_fehler      VARCHAR2(4000);
    BEGIN
        log_prozess_start(p_batch_id, TRUNC(SYSDATE), 'TRANSFORMATION', v_prozess_id);
        BEGIN
            PKG_ETL_VORTABELLEN.run_transformation(p_batch_id);
            log_prozess_ende(v_prozess_id, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_prozess_ende(v_prozess_id, 'FEHLER', v_fehler);
                RAISE;
        END;
    END run_transform_only;


    -- =========================================================================
    -- RUN_ZIELBELADUNG_ONLY (Phase 3 einzeln, z.B. fuer Reprocessing)
    -- =========================================================================
    PROCEDURE run_zielbeladung_only (p_batch_id IN NUMBER) AS
        v_prozess_id  NUMBER;
        v_fehler      VARCHAR2(4000);
    BEGIN
        log_prozess_start(p_batch_id, TRUNC(SYSDATE), 'ZIELBELADUNG', v_prozess_id);
        BEGIN
            PKG_ETL_ZIELTABELLEN.run_zielbeladung(p_batch_id);
            log_prozess_ende(v_prozess_id, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_prozess_ende(v_prozess_id, 'FEHLER', v_fehler);
                RAISE;
        END;
    END run_zielbeladung_only;


    -- =========================================================================
    -- GET_LAST_BATCH_STATUS (Monitoring)
    -- =========================================================================
    FUNCTION get_last_batch_status RETURN t_etl_status AS
        v_result t_etl_status;
    BEGIN
        -- Letzten Gesamtlauf holen
        BEGIN
            SELECT batch_id, lade_datum
            INTO   v_result.batch_id, v_result.lade_datum
            FROM   ETL_PROZESSLOG
            WHERE  phase = 'GESAMT'
            ORDER BY prozess_id DESC
            FETCH FIRST 1 ROW ONLY;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_result.batch_id := NULL;
                RETURN v_result;
        END;

        -- Status je Phase holen
        FOR rec IN (
            SELECT phase, status
            FROM   ETL_PROZESSLOG
            WHERE  batch_id = v_result.batch_id
            AND    phase != 'GESAMT'
        ) LOOP
            CASE rec.phase
                WHEN 'STAGING'        THEN v_result.status_staging   := rec.status;
                WHEN 'TRANSFORMATION' THEN v_result.status_transform  := rec.status;
                WHEN 'ZIELBELADUNG'   THEN v_result.status_zielladen  := rec.status;
                ELSE NULL;
            END CASE;
        END LOOP;

        -- Gesamtstatus
        SELECT status INTO v_result.status_gesamt
        FROM   ETL_PROZESSLOG
        WHERE  batch_id = v_result.batch_id
        AND    phase    = 'GESAMT'
        ORDER BY prozess_id DESC
        FETCH FIRST 1 ROW ONLY;

        RETURN v_result;
    END get_last_batch_status;


    -- =========================================================================
    -- CHECK_DATA_QUALITY (Datenqualitaets-Pruefungen nach dem ETL)
    -- =========================================================================
    PROCEDURE check_data_quality (p_batch_id IN NUMBER) AS
        v_fehler_wertpapier  NUMBER;
        v_fehler_fonds       NUMBER;
        v_fehler_kurs        NUMBER;
        v_fehler_bestand     NUMBER;
        v_fehler_transaktion NUMBER;
        v_fehler_fbwg        NUMBER;
    BEGIN
        -- Zähle ungueltige Datensaetze je Vortabelle
        SELECT COUNT(*) INTO v_fehler_wertpapier
        FROM   VOR_WERTPAPIER WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        SELECT COUNT(*) INTO v_fehler_fonds
        FROM   VOR_FONDS WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        SELECT COUNT(*) INTO v_fehler_kurs
        FROM   VOR_KURS WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        SELECT COUNT(*) INTO v_fehler_bestand
        FROM   VOR_BESTAND WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        SELECT COUNT(*) INTO v_fehler_transaktion
        FROM   VOR_TRANSAKTION WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        SELECT COUNT(*) INTO v_fehler_fbwg
        FROM   VOR_FONDSBEWEGUNG WHERE batch_id = p_batch_id AND ist_gueltig = 'N';

        -- Ausgabe (im produktiven Einsatz: Tabelle/E-Mail statt DBMS_OUTPUT)
        DBMS_OUTPUT.PUT_LINE('============================================');
        DBMS_OUTPUT.PUT_LINE('DATENQUALITAETS-BERICHT - Batch: ' || p_batch_id);
        DBMS_OUTPUT.PUT_LINE('============================================');
        DBMS_OUTPUT.PUT_LINE('Ungueltige Wertpapier-Stammdaten: ' || v_fehler_wertpapier);
        DBMS_OUTPUT.PUT_LINE('Ungueltige Fonds-Stammdaten:      ' || v_fehler_fonds);
        DBMS_OUTPUT.PUT_LINE('Ungueltige Kursdaten:             ' || v_fehler_kurs);
        DBMS_OUTPUT.PUT_LINE('Ungueltige Bestandsdaten:         ' || v_fehler_bestand);
        DBMS_OUTPUT.PUT_LINE('Ungueltige Transaktionsdaten:     ' || v_fehler_transaktion);
        DBMS_OUTPUT.PUT_LINE('Ungueltige Fondsbewegungsdaten:   ' || v_fehler_fbwg);
        DBMS_OUTPUT.PUT_LINE('============================================');

        IF v_fehler_wertpapier + v_fehler_fonds + v_fehler_kurs
           + v_fehler_bestand + v_fehler_transaktion + v_fehler_fbwg > 0 THEN
            DBMS_OUTPUT.PUT_LINE('WARNUNG: Ungueltige Datensaetze vorhanden!');
            DBMS_OUTPUT.PUT_LINE('Pruefen: SELECT * FROM VOR_* WHERE batch_id = '
                || p_batch_id || ' AND ist_gueltig = ''N''');
        ELSE
            DBMS_OUTPUT.PUT_LINE('OK: Alle Datensaetze sind gueltig.');
        END IF;
    END check_data_quality;

END PKG_ETL_CONTROL;
/

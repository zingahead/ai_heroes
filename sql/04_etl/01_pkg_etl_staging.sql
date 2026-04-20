-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- ETL Layer 1: Staging-Ladepaket (PKG_ETL_STAGING)
-- =============================================================================
-- Beschreibung:
--   Dieses Package extrahiert Daten aus den Quellsystemen und laedt sie 1:1
--   in die Staging-Tabellen. Jeder Ladelauf erhaelt eine eindeutige Batch-ID.
--
-- Quellsysteme:
--   FONDSBUCHHALTUNG  -> STG_FB_BESTAND, STG_FB_TRANSAKTION, STG_FB_FONDSBEWEGUNG
--   KURSVERSORGUNG    -> STG_KV_KURS, STG_KV_WECHSELKURS
--   WERTPAPIERSTAMMDATEN -> STG_WP_STAMMDATEN, STG_WP_FONDS_STAMMDATEN
--
-- Hinweis: Die INSERT-Statements in den LOAD_*-Prozeduren sind als
--   Platzhalter mit Beispieldaten angelegt. Im produktiven Einsatz werden
--   diese durch Database Links (DB_LINK) auf die Quellsysteme ersetzt.
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_ETL_STAGING AS

    -- =========================================================================
    -- Oeffentliche Konstanten
    -- =========================================================================
    c_quellsys_fondsbuch   CONSTANT VARCHAR2(30) := 'FONDSBUCHHALTUNG';
    c_quellsys_kursversorg CONSTANT VARCHAR2(30) := 'KURSVERSORGUNG';
    c_quellsys_wertpapier  CONSTANT VARCHAR2(30) := 'WERTPAPIERSTAMMDATEN';

    -- =========================================================================
    -- Oeffentliche Typen
    -- =========================================================================
    TYPE t_ladeergebnis IS RECORD (
        batch_id        NUMBER,
        anzahl_saetze   NUMBER,
        status          VARCHAR2(20),
        fehlermeldung   VARCHAR2(4000)
    );

    -- =========================================================================
    -- Hauptprozeduren
    -- =========================================================================

    -- Vollstaendigen Staging-Ladelauf starten (alle Quellsysteme)
    PROCEDURE run_full_staging (
        p_batch_id      OUT NUMBER,
        p_lade_datum    IN  DATE    DEFAULT TRUNC(SYSDATE)
    );

    -- Einzelne Quellsystem-Ladelaeufe
    PROCEDURE load_fondsbuchhaltung (
        p_batch_id      IN  NUMBER,
        p_lade_datum    IN  DATE    DEFAULT TRUNC(SYSDATE)
    );

    PROCEDURE load_kursversorgung (
        p_batch_id      IN  NUMBER,
        p_lade_datum    IN  DATE    DEFAULT TRUNC(SYSDATE)
    );

    PROCEDURE load_wertpapierstammdaten (
        p_batch_id      IN  NUMBER
    );

    -- Hilfsfunktionen
    FUNCTION get_new_batch_id RETURN NUMBER;

    PROCEDURE log_start (
        p_batch_id      IN NUMBER,
        p_quellsystem   IN VARCHAR2,
        p_tabelle       IN VARCHAR2,
        p_log_id        OUT NUMBER
    );

    PROCEDURE log_ende (
        p_log_id        IN NUMBER,
        p_anzahl_saetze IN NUMBER,
        p_status        IN VARCHAR2,
        p_fehlermeldung IN VARCHAR2 DEFAULT NULL
    );

END PKG_ETL_STAGING;
/


CREATE OR REPLACE PACKAGE BODY PKG_ETL_STAGING AS

    -- =========================================================================
    -- Hilfsfunktionen (privat)
    -- =========================================================================

    -- Neue Batch-ID aus Sequenz holen
    FUNCTION get_new_batch_id RETURN NUMBER AS
        v_batch_id NUMBER;
    BEGIN
        SELECT SEQ_STG_BATCH_ID.NEXTVAL INTO v_batch_id FROM DUAL;
        RETURN v_batch_id;
    END get_new_batch_id;

    -- Ladelauf-Start protokollieren
    PROCEDURE log_start (
        p_batch_id      IN NUMBER,
        p_quellsystem   IN VARCHAR2,
        p_tabelle       IN VARCHAR2,
        p_log_id        OUT NUMBER
    ) AS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO STG_LADELOG (batch_id, quellsystem, tabelle, ladestart, status)
        VALUES (p_batch_id, p_quellsystem, p_tabelle, SYSTIMESTAMP, 'GESTARTET')
        RETURNING log_id INTO p_log_id;
        COMMIT;
    END log_start;

    -- Ladelauf-Ende protokollieren
    PROCEDURE log_ende (
        p_log_id        IN NUMBER,
        p_anzahl_saetze IN NUMBER,
        p_status        IN VARCHAR2,
        p_fehlermeldung IN VARCHAR2 DEFAULT NULL
    ) AS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE STG_LADELOG
        SET    ladeende       = SYSTIMESTAMP,
               anzahl_saetze = p_anzahl_saetze,
               status        = p_status,
               fehlermeldung = SUBSTR(p_fehlermeldung, 1, 4000)
        WHERE  log_id = p_log_id;
        COMMIT;
    END log_ende;


    -- =========================================================================
    -- LOAD_FONDSBUCHHALTUNG
    -- Laedt Bestaende, Transaktionen und Fondsbewegungen aus der Fondsbuchhaltung
    -- =========================================================================
    PROCEDURE load_fondsbuchhaltung (
        p_batch_id   IN NUMBER,
        p_lade_datum IN DATE DEFAULT TRUNC(SYSDATE)
    ) AS
        v_log_id     NUMBER;
        v_anzahl     NUMBER := 0;
        v_fehler     VARCHAR2(4000);
    BEGIN

        -- -----------------------------------------------------------------
        -- 1. Fondsbestaende laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_fondsbuch, 'STG_FB_BESTAND', v_log_id);
        BEGIN
            -- Lade Bestandsdaten fuer das angegebene Datum
            -- Im produktiven Einsatz: Quellsystem via DB-Link (z.B. @FONDSBUCH_DBLINK)
            INSERT INTO STG_FB_BESTAND (
                batch_id, fonds_id, isin, bewertungsdatum,
                nominale, stueckzahl,
                einstandskurs, einstandswert,
                aktueller_kurs, marktwert,
                waehrung_fonds, waehrung_papier, wechselkurs,
                marktwert_fondswährung,
                depot_nr, depotstelle,
                src_erstellt_am, src_geaendert_am
            )
            -- Hier wird die Abfrage gegen das Quellsystem ausgefuehrt.
            -- Beispiel mit DB-Link (anpassen an tatsaechliches Quellsystem):
            -- SELECT p_batch_id, fonds_id, isin, bewertungsdatum, ...
            -- FROM   BESTAND_VIEW@FONDSBUCH_DBLINK
            -- WHERE  bewertungsdatum = p_lade_datum;
            --
            -- Platzhalter fuer Tests (simulierte Quelldaten):
            SELECT p_batch_id,
                   src.fonds_id, src.isin, src.bewertungsdatum,
                   src.nominale, src.stueckzahl,
                   src.einstandskurs, src.einstandswert,
                   src.aktueller_kurs, src.marktwert,
                   src.waehrung_fonds, src.waehrung_papier, src.wechselkurs,
                   src.marktwert * src.wechselkurs AS marktwert_fondswährung,
                   src.depot_nr, src.depotstelle,
                   SYSDATE, SYSDATE
            FROM   (
                -- Testdaten-Simulation (im Produktivsystem: DB-Link-Abfrage)
                SELECT 'FONDS001'   AS fonds_id,
                       'DE0005140008' AS isin,
                       p_lade_datum  AS bewertungsdatum,
                       NULL           AS nominale,
                       1000           AS stueckzahl,
                       95.50          AS einstandskurs,
                       95500.00       AS einstandswert,
                       102.30         AS aktueller_kurs,
                       102300.00      AS marktwert,
                       'EUR'          AS waehrung_fonds,
                       'EUR'          AS waehrung_papier,
                       1.0            AS wechselkurs,
                       'DEPOT001'     AS depot_nr,
                       'Musterbank AG' AS depotstelle
                FROM DUAL
                UNION ALL
                SELECT 'FONDS001', 'US5949181045', p_lade_datum,
                       NULL, 500, 120.00, 60000.00, 145.50, 72750.00,
                       'EUR', 'USD', 0.92, 'DEPOT001', 'Musterbank AG'
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        -- -----------------------------------------------------------------
        -- 2. Transaktionen laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_fondsbuch, 'STG_FB_TRANSAKTION', v_log_id);
        BEGIN
            INSERT INTO STG_FB_TRANSAKTION (
                batch_id, transaktions_id, fonds_id, isin,
                transaktionstyp, handelsdatum, valutatdatum, buchungsdatum,
                stueckzahl, nominale, kurs, kursdatum,
                transaktionswert, provision, stueckzinsen, gesamtbetrag,
                waehrung, waehrung_fonds, wechselkurs,
                gesamtbetrag_fondswährung,
                kontrahent, depot_nr,
                src_transaktions_nr, src_status,
                src_erstellt_am, src_geaendert_am
            )
            -- Im produktiven Einsatz: Abfrage gegen FONDSBUCH_DBLINK
            -- Lade alle Transaktionen des Ladedate oder Delta seit letztem Lauf
            SELECT p_batch_id,
                   src.transaktions_id, src.fonds_id, src.isin,
                   src.transaktionstyp, src.handelsdatum, src.valutatdatum, src.buchungsdatum,
                   src.stueckzahl, src.nominale, src.kurs, src.kursdatum,
                   src.transaktionswert, src.provision, src.stueckzinsen, src.gesamtbetrag,
                   src.waehrung, src.waehrung_fonds, src.wechselkurs,
                   src.gesamtbetrag * src.wechselkurs,
                   src.kontrahent, src.depot_nr,
                   src.transaktions_id, 'ABGESCHLOSSEN',
                   SYSDATE, SYSDATE
            FROM (
                SELECT 'TXN-' || TO_CHAR(p_lade_datum,'YYYYMMDD') || '-001' AS transaktions_id,
                       'FONDS001' AS fonds_id, 'DE0005140008' AS isin,
                       'KAUF' AS transaktionstyp,
                       p_lade_datum AS handelsdatum,
                       p_lade_datum + 2 AS valutatdatum,
                       p_lade_datum + 2 AS buchungsdatum,
                       100 AS stueckzahl, NULL AS nominale,
                       102.30 AS kurs, p_lade_datum AS kursdatum,
                       10230.00 AS transaktionswert,
                       25.00 AS provision, 0 AS stueckzinsen,
                       10255.00 AS gesamtbetrag,
                       'EUR' AS waehrung, 'EUR' AS waehrung_fonds,
                       1.0 AS wechselkurs, 'Musterkontrahent GmbH' AS kontrahent,
                       'DEPOT001' AS depot_nr
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        -- -----------------------------------------------------------------
        -- 3. Fondsbewegungen laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_fondsbuch, 'STG_FB_FONDSBEWEGUNG', v_log_id);
        BEGIN
            INSERT INTO STG_FB_FONDSBEWEGUNG (
                batch_id, bewegungs_id, fonds_id,
                bewegungstyp, handelsdatum, valutatdatum, buchungsdatum,
                anteile, anteilswert_nav, ausgabeaufschlag,
                bruttobetrag, nettobetrag,
                depot_nr, depotstelle, anleger_id,
                waehrung,
                src_bewegungs_nr, src_status,
                src_erstellt_am, src_geaendert_am
            )
            SELECT p_batch_id,
                   src.bewegungs_id, src.fonds_id,
                   src.bewegungstyp, src.handelsdatum, src.valutatdatum, src.buchungsdatum,
                   src.anteile, src.anteilswert_nav, src.ausgabeaufschlag,
                   src.bruttobetrag, src.nettobetrag,
                   src.depot_nr, src.depotstelle, src.anleger_id,
                   src.waehrung,
                   src.bewegungs_id, 'ABGESCHLOSSEN',
                   SYSDATE, SYSDATE
            FROM (
                SELECT 'FBW-' || TO_CHAR(p_lade_datum,'YYYYMMDD') || '-001' AS bewegungs_id,
                       'FONDS001' AS fonds_id,
                       'AUSGABE' AS bewegungstyp,
                       p_lade_datum AS handelsdatum,
                       p_lade_datum + 2 AS valutatdatum,
                       p_lade_datum + 2 AS buchungsdatum,
                       50.0 AS anteile, 210.50 AS anteilswert_nav,
                       0.03 AS ausgabeaufschlag,
                       10857.75 AS bruttobetrag, 10525.00 AS nettobetrag,
                       'DEPOT001' AS depot_nr, 'Musterbank AG' AS depotstelle,
                       'ANLEGER001' AS anleger_id, 'EUR' AS waehrung
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fondsbuchhaltung;


    -- =========================================================================
    -- LOAD_KURSVERSORGUNG
    -- Laedt Marktpreise und Wechselkurse aus der Kursversorgung
    -- =========================================================================
    PROCEDURE load_kursversorgung (
        p_batch_id   IN NUMBER,
        p_lade_datum IN DATE DEFAULT TRUNC(SYSDATE)
    ) AS
        v_log_id  NUMBER;
        v_anzahl  NUMBER := 0;
        v_fehler  VARCHAR2(4000);
    BEGIN

        -- -----------------------------------------------------------------
        -- 1. Kurse laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_kursversorg, 'STG_KV_KURS', v_log_id);
        BEGIN
            INSERT INTO STG_KV_KURS (
                batch_id, isin, kursdatum, kurszeit, kurstyp,
                kurs, waehrung,
                handelsvolumen, umsatz_stueck,
                boerse, boerse_kuerzel,
                kurs_quelle, kurs_qualitaet,
                src_kurs_id, src_erstellt_am
            )
            -- Im produktiven Einsatz: Abfrage gegen KURSVERSORG_DBLINK
            SELECT p_batch_id,
                   src.isin, src.kursdatum, src.kurszeit, src.kurstyp,
                   src.kurs, src.waehrung,
                   src.handelsvolumen, src.umsatz_stueck,
                   src.boerse, src.boerse_kuerzel,
                   src.kurs_quelle, src.kurs_qualitaet,
                   src.kurs_id, SYSTIMESTAMP
            FROM (
                -- Testdaten-Simulation (Schlusskkurse zweier Wertpapiere)
                SELECT 'KURS-' || TO_CHAR(p_lade_datum,'YYYYMMDD') || '-001' AS kurs_id,
                       'DE0005140008' AS isin, p_lade_datum AS kursdatum,
                       '17:30:00' AS kurszeit, 'SCHLUSS' AS kurstyp,
                       102.30 AS kurs, 'EUR' AS waehrung,
                       5000000 AS handelsvolumen, 48900 AS umsatz_stueck,
                       'XETRA' AS boerse, 'ETR' AS boerse_kuerzel,
                       'REUTERS' AS kurs_quelle, 'EOD' AS kurs_qualitaet
                FROM DUAL
                UNION ALL
                SELECT 'KURS-' || TO_CHAR(p_lade_datum,'YYYYMMDD') || '-002',
                       'US5949181045', p_lade_datum, '21:00:00', 'SCHLUSS',
                       145.50, 'USD', 25000000, 171800, 'NASDAQ', 'NAS',
                       'BLOOMBERG', 'EOD'
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        -- -----------------------------------------------------------------
        -- 2. Wechselkurse laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_kursversorg, 'STG_KV_WECHSELKURS', v_log_id);
        BEGIN
            INSERT INTO STG_KV_WECHSELKURS (
                batch_id, waehrung_von, waehrung_nach, kursdatum, kurstyp,
                kurs, src_kurs_id, src_erstellt_am
            )
            SELECT p_batch_id,
                   src.waehrung_von, src.waehrung_nach, src.kursdatum, src.kurstyp,
                   src.kurs,
                   'WK-' || src.waehrung_von || src.waehrung_nach || TO_CHAR(src.kursdatum,'YYYYMMDD'),
                   SYSTIMESTAMP
            FROM (
                SELECT 'EUR' AS waehrung_von, 'USD' AS waehrung_nach,
                       p_lade_datum AS kursdatum, 'EZB' AS kurstyp,
                       1.0873 AS kurs FROM DUAL
                UNION ALL
                SELECT 'EUR', 'GBP', p_lade_datum, 'EZB', 0.8612 FROM DUAL
                UNION ALL
                SELECT 'EUR', 'CHF', p_lade_datum, 'EZB', 0.9523 FROM DUAL
                UNION ALL
                SELECT 'EUR', 'JPY', p_lade_datum, 'EZB', 163.45 FROM DUAL
                UNION ALL
                SELECT 'USD', 'EUR', p_lade_datum, 'EZB', 0.9197 FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_kursversorgung;


    -- =========================================================================
    -- LOAD_WERTPAPIERSTAMMDATEN
    -- Laedt Wertpapier- und Fonds-Stammdaten (Volllieferung oder Delta)
    -- =========================================================================
    PROCEDURE load_wertpapierstammdaten (
        p_batch_id IN NUMBER
    ) AS
        v_log_id  NUMBER;
        v_anzahl  NUMBER := 0;
        v_fehler  VARCHAR2(4000);
    BEGIN

        -- -----------------------------------------------------------------
        -- 1. Wertpapier-Stammdaten laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_wertpapier, 'STG_WP_STAMMDATEN', v_log_id);
        BEGIN
            INSERT INTO STG_WP_STAMMDATEN (
                batch_id, isin, wkn, ticker,
                bezeichnung_lang, bezeichnung_kurz,
                wertpapiertyp, asset_klasse, sub_asset_klasse,
                emittent_name, land_emittent,
                branche, branche_code,
                handelswährung, notizwaehrung,
                hauptboerse, boerse_kuerzel,
                status,
                src_wertpapier_id, src_erstellt_am, src_geaendert_am
            )
            -- Im produktiven Einsatz: Abfrage gegen WERTPAPIER_DBLINK
            SELECT p_batch_id,
                   src.isin, src.wkn, src.ticker,
                   src.bezeichnung_lang, src.bezeichnung_kurz,
                   src.wertpapiertyp, src.asset_klasse, src.sub_asset_klasse,
                   src.emittent_name, src.land_emittent,
                   src.branche, src.branche_code,
                   src.handelswährung, src.notizwaehrung,
                   src.hauptboerse, src.boerse_kuerzel,
                   src.status,
                   src.isin, SYSDATE, SYSDATE
            FROM (
                SELECT 'DE0005140008' AS isin, '514000' AS wkn, 'DBK' AS ticker,
                       'Deutsche Bank AG' AS bezeichnung_lang,
                       'Deutsche Bank' AS bezeichnung_kurz,
                       'AKTIE' AS wertpapiertyp,
                       'AKTIEN' AS asset_klasse, 'GROSSBANKEN' AS sub_asset_klasse,
                       'Deutsche Bank AG' AS emittent_name, 'DEU' AS land_emittent,
                       'Finanzwesen' AS branche, '40' AS branche_code,
                       'EUR' AS handelswährung, 'EUR' AS notizwaehrung,
                       'XETRA' AS hauptboerse, 'ETR' AS boerse_kuerzel,
                       'AKTIV' AS status
                FROM DUAL
                UNION ALL
                SELECT 'US5949181045', NULL, 'MSFT',
                       'Microsoft Corporation', 'Microsoft',
                       'AKTIE', 'AKTIEN', 'SOFTWARE',
                       'Microsoft Corporation', 'USA',
                       'Informationstechnologie', '45',
                       'USD', 'USD', 'NASDAQ', 'NAS', 'AKTIV'
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        -- -----------------------------------------------------------------
        -- 2. Fonds-Stammdaten laden
        -- -----------------------------------------------------------------
        log_start(p_batch_id, c_quellsys_wertpapier, 'STG_WP_FONDS_STAMMDATEN', v_log_id);
        BEGIN
            INSERT INTO STG_WP_FONDS_STAMMDATEN (
                batch_id, fonds_id, isin, wkn,
                fondsname, fondsname_kurz,
                fondstyp, investmentstrategie, asset_klasse,
                fondswaehrung, auflagedatum,
                kag_name, verwahrstelle,
                domizil_land, rechtliche_struktur, ucits_konform,
                status,
                src_fonds_id, src_erstellt_am, src_geaendert_am
            )
            SELECT p_batch_id,
                   src.fonds_id, src.isin, src.wkn,
                   src.fondsname, src.fondsname_kurz,
                   src.fondstyp, src.investmentstrategie, src.asset_klasse,
                   src.fondswaehrung, src.auflagedatum,
                   src.kag_name, src.verwahrstelle,
                   src.domizil_land, src.rechtliche_struktur, src.ucits_konform,
                   src.status,
                   src.fonds_id, SYSDATE, SYSDATE
            FROM (
                SELECT 'FONDS001' AS fonds_id, 'DE000A0M23M0' AS isin,
                       'A0M23M' AS wkn,
                       'Musterfonds Aktien Global' AS fondsname,
                       'Musterfonds AG Global' AS fondsname_kurz,
                       'PUBLIKUMSFONDS' AS fondstyp,
                       'Globale Aktien, Diversifiziert' AS investmentstrategie,
                       'AKTIEN' AS asset_klasse,
                       'EUR' AS fondswaehrung,
                       DATE '2010-01-15' AS auflagedatum,
                       'Muster KAG GmbH' AS kag_name,
                       'Muster Verwahrstelle AG' AS verwahrstelle,
                       'DEU' AS domizil_land,
                       'SONDERVERMÖGEN' AS rechtliche_struktur,
                       'J' AS ucits_konform, 'AKTIV' AS status
                FROM DUAL
            ) src;

            v_anzahl := SQL%ROWCOUNT;
            log_ende(v_log_id, v_anzahl, 'ERFOLGREICH');
        EXCEPTION
            WHEN OTHERS THEN
                v_fehler := SQLERRM;
                log_ende(v_log_id, 0, 'FEHLER', v_fehler);
                RAISE;
        END;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_wertpapierstammdaten;


    -- =========================================================================
    -- RUN_FULL_STAGING
    -- Hauptprozedur: Startet alle Staging-Ladelaeufe in der richtigen Reihenfolge
    -- =========================================================================
    PROCEDURE run_full_staging (
        p_batch_id   OUT NUMBER,
        p_lade_datum IN  DATE DEFAULT TRUNC(SYSDATE)
    ) AS
        v_batch_id  NUMBER;
    BEGIN
        -- Neue Batch-ID vergeben
        v_batch_id := get_new_batch_id();
        p_batch_id := v_batch_id;

        -- Reihenfolge: Stammdaten zuerst, dann Bewegungsdaten
        -- 1. Wertpapier-Stammdaten (Basis fuer alle weiteren Daten)
        load_wertpapierstammdaten(v_batch_id);

        -- 2. Kurse (benoetigt ISIN aus Stammdaten)
        load_kursversorgung(v_batch_id, p_lade_datum);

        -- 3. Fondsbuchhaltungs-Daten (Bestaende, Transaktionen, Bewegungen)
        load_fondsbuchhaltung(v_batch_id, p_lade_datum);

    EXCEPTION
        WHEN OTHERS THEN
            -- Fehler protokollieren, Batch-ID zurueckgeben fuer Diagnose
            p_batch_id := NVL(v_batch_id, -1);
            RAISE;
    END run_full_staging;

END PKG_ETL_STAGING;
/

-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- ETL Layer 3: Zieltabellen-Beladung (PKG_ETL_ZIELTABELLEN)
-- =============================================================================
-- Beschreibung:
--   Dieses Package belaedt die Dimensions- und Faktentabellen aus den
--   Vortabellen. Dabei werden:
--     - Dimensionstabellen nach SCD Typ 2 Logik befuellt
--     - Surrogatschluessel aufgeloest
--     - Faktentabellen mit MERGE (Upsert) befuellt
--     - Referentielle Integritaet sichergestellt
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_ETL_ZIELTABELLEN AS

    -- Vollstaendige Beladung aller Zieltabellen aus den Vortabellen
    PROCEDURE run_zielbeladung (
        p_batch_id  IN NUMBER
    );

    -- Dimensionsbeladung
    PROCEDURE load_dim_wertpapier (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_dim_fonds (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_dim_depot (
        p_batch_id  IN NUMBER
    );

    -- Faktentabellen-Beladung
    PROCEDURE load_fakt_kurs (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_fakt_wechselkurs (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_fakt_bestand (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_fakt_transaktion (
        p_batch_id  IN NUMBER
    );

    PROCEDURE load_fakt_fondsbewegung (
        p_batch_id  IN NUMBER
    );

END PKG_ETL_ZIELTABELLEN;
/


CREATE OR REPLACE PACKAGE BODY PKG_ETL_ZIELTABELLEN AS

    -- =========================================================================
    -- Hilfsfunktionen (privat)
    -- =========================================================================

    -- Holt Wertpapier-SK (Surrogatschluessel) fuer eine ISIN (aktueller Datensatz)
    FUNCTION get_wertpapier_sk (p_isin IN VARCHAR2) RETURN NUMBER AS
        v_sk NUMBER;
    BEGIN
        SELECT wertpapier_sk INTO v_sk
        FROM   DIM_WERTPAPIER
        WHERE  isin       = p_isin
        AND    ist_aktuell = 'J'
        AND    ROWNUM = 1;
        RETURN v_sk;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END get_wertpapier_sk;

    -- Holt Fonds-SK fuer eine Fonds-ID (aktueller Datensatz)
    FUNCTION get_fonds_sk (p_fonds_id IN VARCHAR2) RETURN NUMBER AS
        v_sk NUMBER;
    BEGIN
        SELECT fonds_sk INTO v_sk
        FROM   DIM_FONDS
        WHERE  fonds_id   = p_fonds_id
        AND    ist_aktuell = 'J'
        AND    ROWNUM = 1;
        RETURN v_sk;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END get_fonds_sk;

    -- Holt Zeit-ID fuer ein Datum (Format YYYYMMDD)
    FUNCTION get_zeit_id (p_datum IN DATE) RETURN NUMBER AS
    BEGIN
        IF p_datum IS NULL THEN
            RETURN NULL;
        END IF;
        RETURN TO_NUMBER(TO_CHAR(p_datum, 'YYYYMMDD'));
    END get_zeit_id;

    -- Holt Waehrungs-ID fuer einen ISO-Code
    FUNCTION get_waehrung_id (p_iso_code IN VARCHAR2) RETURN NUMBER AS
        v_id NUMBER;
    BEGIN
        SELECT waehrung_id INTO v_id
        FROM   DIM_WAEHRUNG
        WHERE  iso_code   = UPPER(TRIM(p_iso_code))
        AND    ist_aktuell = 'J'
        AND    ROWNUM = 1;
        RETURN v_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END get_waehrung_id;

    -- Holt Land-ID fuer einen ISO3-Code
    FUNCTION get_land_id (p_iso3_code IN VARCHAR2) RETURN NUMBER AS
        v_id NUMBER;
    BEGIN
        SELECT land_id INTO v_id
        FROM   DIM_LAND
        WHERE  iso3_code  = UPPER(TRIM(p_iso3_code))
        AND    ist_aktuell = 'J'
        AND    ROWNUM = 1;
        RETURN v_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END get_land_id;

    -- Holt Branchen-ID fuer einen GICS-Sektor-Code oder internen Code
    FUNCTION get_branche_id (p_branche_code IN VARCHAR2) RETURN NUMBER AS
        v_id NUMBER;
    BEGIN
        SELECT branche_id INTO v_id
        FROM   DIM_BRANCHE
        WHERE  (gics_sektor_code = p_branche_code
                OR branche_intern_code = UPPER(TRIM(p_branche_code)))
        AND    ist_aktuell = 'J'
        AND    ROWNUM = 1;
        RETURN v_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END get_branche_id;


    -- =========================================================================
    -- LOAD_DIM_WERTPAPIER
    -- SCD Typ 2: Neue Wertpapiere einfuegen, geaenderte historisieren
    -- =========================================================================
    PROCEDURE load_dim_wertpapier (p_batch_id IN NUMBER) AS
        v_heute DATE := TRUNC(SYSDATE);
    BEGIN
        -- Schritt 1: Geaenderte Datensaetze historisieren
        --   Vorhandene aktuelle Datensaetze schliessen, wenn sich relevante
        --   Attribute gegenueber der Vortabelle geaendert haben
        UPDATE DIM_WERTPAPIER dw
        SET    gueltig_bis   = v_heute - 1,
               ist_aktuell  = 'N',
               geaendert_am = SYSTIMESTAMP,
               batch_id_geaendert = p_batch_id
        WHERE  dw.ist_aktuell = 'J'
        AND    EXISTS (
            SELECT 1
            FROM   VOR_WERTPAPIER vw
            WHERE  vw.batch_id   = p_batch_id
            AND    vw.isin       = dw.isin
            AND    vw.ist_gueltig = 'J'
            AND (  -- SCD Typ 2: Aenderung in relevanten Attributen
                   NVL(vw.bezeichnung_lang,'~')  <> NVL(dw.bezeichnung_lang,'~')
                OR NVL(vw.wertpapiertyp,'~')     <> NVL(dw.wertpapiertyp,'~')
                OR NVL(vw.asset_klasse,'~')       <> NVL(dw.asset_klasse,'~')
                OR NVL(vw.emittent_name,'~')      <> NVL(dw.emittent_name,'~')
                OR NVL(vw.land_iso3,'~')          <> NVL(dw.land_iso3,'~')
                OR NVL(vw.faelligkeitsdatum,DATE '1900-01-01')
                   <> NVL(dw.faelligkeitsdatum,DATE '1900-01-01')
            )
        );

        -- Schritt 2: Neue und geaenderte Datensaetze einfuegen
        INSERT INTO DIM_WERTPAPIER (
            gueltig_von, gueltig_bis, ist_aktuell, dwh_version,
            isin, wkn, valor, ticker,
            bezeichnung_lang, bezeichnung_kurz,
            wertpapiertyp, asset_klasse, sub_asset_klasse,
            emittent_name,
            land_id, branche_id, waehrung_id,
            nominalwert, nominalwaehrung, faelligkeitsdatum,
            kuponrate, kuponfrequenz,
            fondstyp, ausschuettungsart, auflagedatum,
            hauptboerse,
            erstellt_am, geaendert_am,
            batch_id_erstellt, batch_id_geaendert
        )
        SELECT
            v_heute,                  -- gueltig_von
            DATE '9999-12-31',        -- gueltig_bis
            'J',                      -- ist_aktuell
            NVL((SELECT MAX(dw2.dwh_version) + 1
                 FROM   DIM_WERTPAPIER dw2
                 WHERE  dw2.isin = vw.isin), 1),
            vw.isin,
            vw.wkn,
            vw.valor,
            vw.ticker,
            vw.bezeichnung_lang,
            vw.bezeichnung_kurz,
            vw.wertpapiertyp,
            vw.asset_klasse,
            vw.sub_asset_klasse,
            vw.emittent_name,
            get_land_id(vw.land_iso3),
            get_branche_id(vw.branche_code),
            get_waehrung_id(vw.handelswährung),
            vw.nominalwert,
            vw.nominalwaehrung,
            vw.faelligkeitsdatum,
            vw.kuponrate,
            vw.kuponfrequenz,
            vw.fondstyp,
            vw.ausschuettungsart,
            vw.auflagedatum,
            vw.hauptboerse,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            p_batch_id,
            p_batch_id
        FROM   VOR_WERTPAPIER vw
        WHERE  vw.batch_id   = p_batch_id
        AND    vw.ist_gueltig = 'J'
        -- Nur neue oder geaenderte (historisierte) einfuegen
        AND NOT EXISTS (
            SELECT 1
            FROM   DIM_WERTPAPIER dw
            WHERE  dw.isin       = vw.isin
            AND    dw.ist_aktuell = 'J'
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_dim_wertpapier;


    -- =========================================================================
    -- LOAD_DIM_FONDS
    -- SCD Typ 2: Neue Fonds einfuegen, geaenderte historisieren
    -- =========================================================================
    PROCEDURE load_dim_fonds (p_batch_id IN NUMBER) AS
        v_heute DATE := TRUNC(SYSDATE);
    BEGIN
        -- Schritt 1: Geaenderte Datensaetze historisieren
        UPDATE DIM_FONDS df
        SET    gueltig_bis        = v_heute - 1,
               ist_aktuell       = 'N',
               geaendert_am      = SYSTIMESTAMP,
               batch_id_geaendert = p_batch_id
        WHERE  df.ist_aktuell = 'J'
        AND    EXISTS (
            SELECT 1
            FROM   VOR_FONDS vf
            WHERE  vf.batch_id   = p_batch_id
            AND    vf.fonds_id   = df.fonds_id
            AND    vf.ist_gueltig = 'J'
            AND (  NVL(vf.fondsname,'~')           <> NVL(df.fondsname,'~')
                OR NVL(vf.fondstyp,'~')            <> NVL(df.fondstyp,'~')
                OR NVL(vf.fondswaehrung,'~')       <> NVL(TO_CHAR(df.fondswaehrung_id),'~')
                OR NVL(vf.kag_name,'~')            <> NVL(df.kag_name,'~')
                OR NVL(vf.investmentstrategie,'~') <> NVL(df.investmentstrategie,'~')
            )
        );

        -- Schritt 2: Neue und geaenderte Datensaetze einfuegen
        INSERT INTO DIM_FONDS (
            gueltig_von, gueltig_bis, ist_aktuell, dwh_version,
            fonds_id, isin, wkn,
            fondsname, fondsname_kurz,
            fondstyp, investmentstrategie, asset_klasse,
            fondswaehrung_id, domizil_land_id,
            auflagedatum,
            kag_name, verwahrstelle,
            rechtliche_struktur, ucits_konform,
            erstellt_am, geaendert_am,
            batch_id_erstellt, batch_id_geaendert
        )
        SELECT
            v_heute,
            DATE '9999-12-31',
            'J',
            NVL((SELECT MAX(df2.dwh_version) + 1
                 FROM   DIM_FONDS df2
                 WHERE  df2.fonds_id = vf.fonds_id), 1),
            vf.fonds_id,
            vf.isin,
            vf.wkn,
            vf.fondsname,
            vf.fondsname_kurz,
            vf.fondstyp,
            vf.investmentstrategie,
            vf.asset_klasse,
            get_waehrung_id(vf.fondswaehrung),
            get_land_id(vf.domizil_land),
            vf.auflagedatum,
            vf.kag_name,
            vf.verwahrstelle,
            vf.rechtliche_struktur,
            vf.ucits_konform,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            p_batch_id,
            p_batch_id
        FROM   VOR_FONDS vf
        WHERE  vf.batch_id   = p_batch_id
        AND    vf.ist_gueltig = 'J'
        AND NOT EXISTS (
            SELECT 1
            FROM   DIM_FONDS df
            WHERE  df.fonds_id  = vf.fonds_id
            AND    df.ist_aktuell = 'J'
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_dim_fonds;


    -- =========================================================================
    -- LOAD_DIM_DEPOT
    -- Laedt neue Depots aus den Bestands- und Transaktionsdaten
    -- =========================================================================
    PROCEDURE load_dim_depot (p_batch_id IN NUMBER) AS
        v_heute DATE := TRUNC(SYSDATE);
    BEGIN
        -- Neue Depots aus Bestandsdaten einfuegen
        INSERT INTO DIM_DEPOT (
            gueltig_von, gueltig_bis, ist_aktuell, dwh_version,
            depot_nr, depotstelle,
            erstellt_am, geaendert_am
        )
        SELECT DISTINCT
            v_heute, DATE '9999-12-31', 'J', 1,
            vb.depot_nr,
            vb.depotstelle,
            SYSTIMESTAMP, SYSTIMESTAMP
        FROM   VOR_BESTAND vb
        WHERE  vb.batch_id   = p_batch_id
        AND    vb.depot_nr IS NOT NULL
        AND    NOT EXISTS (
            SELECT 1
            FROM   DIM_DEPOT dd
            WHERE  dd.depot_nr   = vb.depot_nr
            AND    dd.ist_aktuell = 'J'
        );

        -- Neue Depots aus Transaktionsdaten einfuegen
        INSERT INTO DIM_DEPOT (
            gueltig_von, gueltig_bis, ist_aktuell, dwh_version,
            depot_nr, erstellt_am, geaendert_am
        )
        SELECT DISTINCT
            v_heute, DATE '9999-12-31', 'J', 1,
            vt.depot_nr,
            SYSTIMESTAMP, SYSTIMESTAMP
        FROM   VOR_TRANSAKTION vt
        WHERE  vt.batch_id   = p_batch_id
        AND    vt.depot_nr IS NOT NULL
        AND    NOT EXISTS (
            SELECT 1
            FROM   DIM_DEPOT dd
            WHERE  dd.depot_nr   = vt.depot_nr
            AND    dd.ist_aktuell = 'J'
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_dim_depot;


    -- =========================================================================
    -- LOAD_FAKT_KURS
    -- Laedt Kursdaten aus VOR_KURS in FAKT_KURS
    -- =========================================================================
    PROCEDURE load_fakt_kurs (p_batch_id IN NUMBER) AS
    BEGIN
        -- MERGE: Vorhandene Kurse aktualisieren, neue einfuegen
        MERGE INTO FAKT_KURS fk
        USING (
            SELECT
                vk.isin,
                vk.kursdatum,
                vk.kurstyp,
                vk.boerse,
                get_zeit_id(vk.kursdatum)      AS zeit_id,
                get_wertpapier_sk(vk.isin)      AS wertpapier_sk,
                get_waehrung_id(vk.waehrung)    AS waehrung_id,
                vk.kurs,
                vk.kurs_in_eur,
                vk.wechselkurs_eur,
                vk.kurs_quelle,
                vk.kurs_qualitaet,
                p_batch_id                      AS batch_id
            FROM   VOR_KURS vk
            WHERE  vk.batch_id   = p_batch_id
            AND    vk.ist_gueltig = 'J'
        ) src
        ON (
            fk.wertpapier_sk = src.wertpapier_sk
            AND fk.zeit_id   = src.zeit_id
            AND fk.kurstyp   = src.kurstyp
            AND NVL(fk.boerse,'XDEFAULT') = NVL(src.boerse,'XDEFAULT')
        )
        WHEN MATCHED THEN UPDATE SET
            fk.kurs          = src.kurs,
            fk.kurs_in_eur   = src.kurs_in_eur,
            fk.wechselkurs_eur = src.wechselkurs_eur,
            fk.kurs_quelle   = src.kurs_quelle,
            fk.kurs_qualitaet = src.kurs_qualitaet,
            fk.erstellt_am   = SYSTIMESTAMP,
            fk.batch_id      = src.batch_id
        WHEN NOT MATCHED THEN INSERT (
            zeit_id, wertpapier_sk, waehrung_id,
            kurstyp, kurs, kurs_in_eur, wechselkurs_eur,
            boerse, kurs_quelle, kurs_qualitaet,
            erstellt_am, batch_id, isin
        ) VALUES (
            src.zeit_id, src.wertpapier_sk, src.waehrung_id,
            src.kurstyp, src.kurs, src.kurs_in_eur, src.wechselkurs_eur,
            src.boerse, src.kurs_quelle, src.kurs_qualitaet,
            SYSTIMESTAMP, src.batch_id, src.isin
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fakt_kurs;


    -- =========================================================================
    -- LOAD_FAKT_WECHSELKURS
    -- =========================================================================
    PROCEDURE load_fakt_wechselkurs (p_batch_id IN NUMBER) AS
    BEGIN
        MERGE INTO FAKT_WECHSELKURS fw
        USING (
            SELECT
                get_zeit_id(vw.kursdatum)          AS zeit_id,
                get_waehrung_id(vw.waehrung_von)   AS waehrung_von_id,
                get_waehrung_id(vw.waehrung_nach)  AS waehrung_nach_id,
                vw.kurstyp,
                vw.kurs,
                p_batch_id                          AS batch_id
            FROM   VOR_WECHSELKURS vw
            WHERE  vw.batch_id   = p_batch_id
            AND    vw.ist_gueltig = 'J'
        ) src
        ON (
            fw.waehrung_von_id  = src.waehrung_von_id
            AND fw.waehrung_nach_id = src.waehrung_nach_id
            AND fw.zeit_id          = src.zeit_id
            AND fw.kurstyp          = src.kurstyp
        )
        WHEN MATCHED THEN UPDATE SET
            fw.kurs       = src.kurs,
            fw.erstellt_am = SYSTIMESTAMP,
            fw.batch_id   = src.batch_id
        WHEN NOT MATCHED THEN INSERT (
            zeit_id, waehrung_von_id, waehrung_nach_id,
            kurstyp, kurs,
            erstellt_am, batch_id
        ) VALUES (
            src.zeit_id, src.waehrung_von_id, src.waehrung_nach_id,
            src.kurstyp, src.kurs,
            SYSTIMESTAMP, src.batch_id
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fakt_wechselkurs;


    -- =========================================================================
    -- LOAD_FAKT_BESTAND
    -- Laedt Bestandsdaten (Snapshot: gleicher Tag wird ueberschrieben)
    -- =========================================================================
    PROCEDURE load_fakt_bestand (p_batch_id IN NUMBER) AS
    BEGIN
        MERGE INTO FAKT_BESTAND fb
        USING (
            SELECT
                get_zeit_id(vb.bewertungsdatum)    AS zeit_id,
                get_fonds_sk(vb.fonds_id)           AS fonds_sk,
                get_wertpapier_sk(vb.isin)          AS wertpapier_sk,
                (SELECT depot_sk FROM DIM_DEPOT
                 WHERE  depot_nr   = vb.depot_nr
                 AND    ist_aktuell = 'J'
                 AND    ROWNUM = 1)                  AS depot_sk,
                get_waehrung_id(vb.waehrung_papier) AS waehrung_papier_id,
                get_waehrung_id(vb.waehrung_fonds)  AS waehrung_fonds_id,
                vb.nominale,
                vb.stueckzahl,
                vb.einstandskurs,
                vb.einstandswert,
                vb.aktueller_kurs,
                vb.marktwert,
                vb.marktwert - NVL(vb.einstandswert, 0) AS unrealisierter_gv,
                vb.marktwert_fondswährung,
                vb.einstandswert                    AS einstandswert_fondswährung,
                vb.wechselkurs,
                vb.marktwert_eur,
                vb.einstandswert * NVL(vb.wechselkurs_eur, 1) AS einstandswert_eur,
                vb.wechselkurs_eur,
                vb.portfoliogewicht,
                p_batch_id  AS batch_id,
                vb.fonds_id,
                vb.isin
            FROM   VOR_BESTAND vb
            WHERE  vb.batch_id   = p_batch_id
            AND    vb.ist_gueltig = 'J'
        ) src
        ON (
            fb.fonds_sk      = src.fonds_sk
            AND fb.wertpapier_sk = src.wertpapier_sk
            AND fb.zeit_id       = src.zeit_id
            AND NVL(fb.depot_sk, 0) = NVL(src.depot_sk, 0)
        )
        WHEN MATCHED THEN UPDATE SET
            fb.nominale             = src.nominale,
            fb.stueckzahl           = src.stueckzahl,
            fb.einstandskurs        = src.einstandskurs,
            fb.einstandswert        = src.einstandswert,
            fb.aktueller_kurs       = src.aktueller_kurs,
            fb.marktwert            = src.marktwert,
            fb.unrealisierter_gv    = src.unrealisierter_gv,
            fb.marktwert_fondswährung = src.marktwert_fondswährung,
            fb.einstandswert_fondswährung = src.einstandswert_fondswährung,
            fb.wechselkurs          = src.wechselkurs,
            fb.marktwert_eur        = src.marktwert_eur,
            fb.einstandswert_eur    = src.einstandswert_eur,
            fb.wechselkurs_eur      = src.wechselkurs_eur,
            fb.portfoliogewicht     = src.portfoliogewicht,
            fb.erstellt_am          = SYSTIMESTAMP,
            fb.batch_id             = src.batch_id
        WHEN NOT MATCHED THEN INSERT (
            zeit_id, fonds_sk, wertpapier_sk, depot_sk,
            waehrung_papier_id, waehrung_fonds_id,
            nominale, stueckzahl,
            einstandskurs, einstandswert,
            aktueller_kurs, marktwert, unrealisierter_gv,
            marktwert_fondswährung, einstandswert_fondswährung, wechselkurs,
            marktwert_eur, einstandswert_eur, wechselkurs_eur,
            portfoliogewicht,
            erstellt_am, batch_id, fonds_id, isin
        ) VALUES (
            src.zeit_id, src.fonds_sk, src.wertpapier_sk, src.depot_sk,
            src.waehrung_papier_id, src.waehrung_fonds_id,
            src.nominale, src.stueckzahl,
            src.einstandskurs, src.einstandswert,
            src.aktueller_kurs, src.marktwert, src.unrealisierter_gv,
            src.marktwert_fondswährung, src.einstandswert_fondswährung, src.wechselkurs,
            src.marktwert_eur, src.einstandswert_eur, src.wechselkurs_eur,
            src.portfoliogewicht,
            SYSTIMESTAMP, src.batch_id, src.fonds_id, src.isin
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fakt_bestand;


    -- =========================================================================
    -- LOAD_FAKT_TRANSAKTION
    -- Laedt Transaktionen (Append-Logik, keine Duplikate)
    -- =========================================================================
    PROCEDURE load_fakt_transaktion (p_batch_id IN NUMBER) AS
    BEGIN
        INSERT INTO FAKT_TRANSAKTION (
            zeit_id_handel, zeit_id_valuta, zeit_id_buchung,
            fonds_sk, wertpapier_sk, depot_sk, waehrung_id,
            transaktions_id,
            transaktionstyp, transaktionstyp_normiert,
            stueckzahl, nominale,
            kurs, transaktionswert, provision, stueckzinsen, gesamtbetrag,
            vorzeichen,
            gesamtbetrag_eur, wechselkurs_eur,
            kontrahent,
            erstellt_am, batch_id, fonds_id, isin
        )
        SELECT
            get_zeit_id(vt.handelsdatum),
            get_zeit_id(vt.valutatdatum),
            get_zeit_id(vt.buchungsdatum),
            get_fonds_sk(vt.fonds_id),
            get_wertpapier_sk(vt.isin),
            (SELECT depot_sk FROM DIM_DEPOT
             WHERE  depot_nr   = vt.depot_nr
             AND    ist_aktuell = 'J'
             AND    ROWNUM = 1),
            get_waehrung_id(vt.waehrung),
            vt.transaktions_id,
            vt.transaktionstyp,
            vt.transaktionstyp_normiert,
            vt.stueckzahl,
            vt.nominale,
            vt.kurs,
            vt.transaktionswert,
            vt.provision,
            vt.stueckzinsen,
            vt.gesamtbetrag,
            -- Vorzeichen: KAUF=+1 (Wertpapier-Zugang), VERKAUF=-1
            CASE vt.transaktionstyp_normiert
                WHEN 'KAUF'    THEN  1
                WHEN 'VERKAUF' THEN -1
                ELSE                  0
            END,
            vt.gesamtbetrag_eur,
            vt.wechselkurs_eur,
            vt.kontrahent,
            SYSTIMESTAMP,
            p_batch_id,
            vt.fonds_id,
            vt.isin
        FROM   VOR_TRANSAKTION vt
        WHERE  vt.batch_id   = p_batch_id
        AND    vt.ist_gueltig = 'J'
        -- Keine Duplikate einfuegen (Transaktions-ID + Fonds-ID als Schluessel)
        AND NOT EXISTS (
            SELECT 1
            FROM   FAKT_TRANSAKTION ft
            WHERE  ft.transaktions_id = vt.transaktions_id
            AND    ft.fonds_sk        = get_fonds_sk(vt.fonds_id)
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fakt_transaktion;


    -- =========================================================================
    -- LOAD_FAKT_FONDSBEWEGUNG
    -- Laedt Fondsbewegungen (Append-Logik, keine Duplikate)
    -- =========================================================================
    PROCEDURE load_fakt_fondsbewegung (p_batch_id IN NUMBER) AS
    BEGIN
        INSERT INTO FAKT_FONDSBEWEGUNG (
            zeit_id_handel, zeit_id_valuta, zeit_id_buchung,
            fonds_sk, depot_sk, waehrung_id,
            bewegungs_id,
            bewegungstyp, bewegungstyp_normiert,
            anteile, anteilswert_nav,
            ausgabeaufschlag, bruttobetrag, nettobetrag,
            vorzeichen,
            nettobetrag_eur, wechselkurs_eur,
            anleger_id,
            erstellt_am, batch_id, fonds_id
        )
        SELECT
            get_zeit_id(vf.handelsdatum),
            get_zeit_id(vf.valutatdatum),
            get_zeit_id(vf.buchungsdatum),
            get_fonds_sk(vf.fonds_id),
            (SELECT depot_sk FROM DIM_DEPOT
             WHERE  depot_nr   = vf.depot_nr
             AND    ist_aktuell = 'J'
             AND    ROWNUM = 1),
            get_waehrung_id(vf.waehrung),
            vf.bewegungs_id,
            vf.bewegungstyp,
            vf.bewegungstyp_normiert,
            vf.anteile,
            vf.anteilswert_nav,
            vf.ausgabeaufschlag,
            vf.bruttobetrag,
            vf.nettobetrag,
            -- Vorzeichen: ZUFLUSS=+1 (Ausgabe), ABFLUSS=-1 (Ruecknahme)
            CASE vf.bewegungstyp_normiert
                WHEN 'ZUFLUSS' THEN  1
                WHEN 'ABFLUSS' THEN -1
                ELSE                  0
            END,
            vf.nettobetrag_eur,
            vf.wechselkurs_eur,
            vf.anleger_id,
            SYSTIMESTAMP,
            p_batch_id,
            vf.fonds_id
        FROM   VOR_FONDSBEWEGUNG vf
        WHERE  vf.batch_id   = p_batch_id
        AND    vf.ist_gueltig = 'J'
        -- Keine Duplikate
        AND NOT EXISTS (
            SELECT 1
            FROM   FAKT_FONDSBEWEGUNG ff
            WHERE  ff.bewegungs_id = vf.bewegungs_id
            AND    ff.fonds_sk     = get_fonds_sk(vf.fonds_id)
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load_fakt_fondsbewegung;


    -- =========================================================================
    -- RUN_ZIELBELADUNG
    -- Hauptprozedur: Beladung aller Zieltabellen in der richtigen Reihenfolge
    -- =========================================================================
    PROCEDURE run_zielbeladung (p_batch_id IN NUMBER) AS
    BEGIN
        -- 1. Dimensionstabellen befuellen (Stammdaten)
        load_dim_wertpapier(p_batch_id);
        load_dim_fonds(p_batch_id);
        load_dim_depot(p_batch_id);

        -- 2. Kurse und Wechselkurse (benoetigen Wertpapier-SKs aus Dim)
        load_fakt_wechselkurs(p_batch_id);
        load_fakt_kurs(p_batch_id);

        -- 3. Bewegungsdaten (benoetigen Fonds-SKs und Wertpapier-SKs)
        load_fakt_bestand(p_batch_id);
        load_fakt_transaktion(p_batch_id);
        load_fakt_fondsbewegung(p_batch_id);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END run_zielbeladung;

END PKG_ETL_ZIELTABELLEN;
/

-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- ETL Layer 2: Vortabellen-Transformation (PKG_ETL_VORTABELLEN)
-- =============================================================================
-- Beschreibung:
--   Dieses Package transformiert und konsolidiert die Rohdaten aus den
--   Staging-Tabellen und schreibt sie in die Vortabellen. Dabei werden:
--     - Datenvalidierungen durchgefuehrt
--     - Geschaeftsregeln angewendet (z.B. Typ-Normierung)
--     - Kursumrechnungen in EUR vorgenommen
--     - Duplikate erkannt und behandelt
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_ETL_VORTABELLEN AS

    -- Vollstaendige Transformation eines Batches (alle Vortabellen)
    PROCEDURE run_transformation (
        p_batch_id  IN NUMBER
    );

    -- Einzelne Transformationen je Themenbereich
    PROCEDURE transform_wertpapier (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_fonds (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_kurse (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_wechselkurse (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_bestaende (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_transaktionen (
        p_batch_id  IN NUMBER
    );

    PROCEDURE transform_fondsbewegungen (
        p_batch_id  IN NUMBER
    );

END PKG_ETL_VORTABELLEN;
/


CREATE OR REPLACE PACKAGE BODY PKG_ETL_VORTABELLEN AS

    -- =========================================================================
    -- Hilfsfunktionen (privat)
    -- =========================================================================

    -- Validiert eine ISIN (12-stellig, Laendercode + 9 alphanum + 1 Pruefziffer)
    FUNCTION validate_isin (p_isin IN VARCHAR2) RETURN VARCHAR2 AS
    BEGIN
        IF p_isin IS NULL THEN
            RETURN 'ISIN fehlt';
        END IF;
        IF LENGTH(p_isin) <> 12 THEN
            RETURN 'ISIN muss 12 Zeichen lang sein (aktuell: ' || LENGTH(p_isin) || ')';
        END IF;
        IF NOT REGEXP_LIKE(p_isin, '^[A-Z]{2}[A-Z0-9]{9}[0-9]$') THEN
            RETURN 'ISIN hat ungueliges Format (erwartet: 2 Grossbuchstaben + 9 alphanum + 1 Ziffer)';
        END IF;
        RETURN NULL; -- Kein Fehler
    END validate_isin;

    -- Normiert Transaktionstypen aus verschiedenen Quellsystemen
    FUNCTION normiere_transaktionstyp (p_typ IN VARCHAR2) RETURN VARCHAR2 AS
    BEGIN
        RETURN CASE UPPER(TRIM(p_typ))
            WHEN 'KAUF'           THEN 'KAUF'
            WHEN 'KAUFAUFTRAG'    THEN 'KAUF'
            WHEN 'BUY'            THEN 'KAUF'
            WHEN 'PURCHASE'       THEN 'KAUF'
            WHEN 'VERKAUF'        THEN 'VERKAUF'
            WHEN 'VERKAUFSAUFTRAG' THEN 'VERKAUF'
            WHEN 'SELL'           THEN 'VERKAUF'
            WHEN 'SALE'           THEN 'VERKAUF'
            WHEN 'TAUSCH'         THEN 'TAUSCH'
            WHEN 'SWAP'           THEN 'TAUSCH'
            ELSE                       'SONSTIGE'
        END;
    END normiere_transaktionstyp;

    -- Normiert Fondsbewegungstypen
    FUNCTION normiere_bewegungstyp (p_typ IN VARCHAR2) RETURN VARCHAR2 AS
    BEGIN
        RETURN CASE UPPER(TRIM(p_typ))
            WHEN 'AUSGABE'        THEN 'ZUFLUSS'
            WHEN 'ZEICHNUNG'      THEN 'ZUFLUSS'
            WHEN 'SUBSCRIPTION'   THEN 'ZUFLUSS'
            WHEN 'RUECKNAHME'     THEN 'ABFLUSS'
            WHEN 'REDEMPTION'     THEN 'ABFLUSS'
            WHEN 'RUECKGABE'      THEN 'ABFLUSS'
            WHEN 'AUSSCHUETTUNG'  THEN 'NEUTRAL'
            WHEN 'THESAURIERUNG'  THEN 'NEUTRAL'
            WHEN 'DIVIDENDE'      THEN 'NEUTRAL'
            ELSE                       'NEUTRAL'
        END;
    END normiere_bewegungstyp;

    -- Holt den EUR-Wechselkurs fuer eine Waehrung und ein Datum aus der Staging-Tabelle
    FUNCTION get_eur_wechselkurs (
        p_waehrung  IN VARCHAR2,
        p_datum     IN DATE,
        p_batch_id  IN NUMBER
    ) RETURN NUMBER AS
        v_kurs NUMBER;
    BEGIN
        IF p_waehrung = 'EUR' THEN
            RETURN 1.0;
        END IF;

        -- Zuerst aus dem aktuellen Batch suchen
        BEGIN
            SELECT kurs INTO v_kurs
            FROM   STG_KV_WECHSELKURS
            WHERE  waehrung_von  = p_waehrung
            AND    waehrung_nach = 'EUR'
            AND    kursdatum     = p_datum
            AND    batch_id      = p_batch_id
            AND    ROWNUM        = 1;
            RETURN v_kurs;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        -- Fallback: Letzten verfuegbaren Kurs suchen (bis zu 5 Tage zurueck)
        BEGIN
            SELECT kurs INTO v_kurs
            FROM   STG_KV_WECHSELKURS
            WHERE  waehrung_von  = p_waehrung
            AND    waehrung_nach = 'EUR'
            AND    kursdatum     = (
                SELECT MAX(kursdatum)
                FROM   STG_KV_WECHSELKURS
                WHERE  waehrung_von  = p_waehrung
                AND    waehrung_nach = 'EUR'
                AND    kursdatum BETWEEN p_datum - 5 AND p_datum
            )
            AND    ROWNUM = 1;
            RETURN v_kurs;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RETURN NULL; -- Kein Kurs verfuegbar
        END;
    END get_eur_wechselkurs;


    -- =========================================================================
    -- TRANSFORM_WERTPAPIER
    -- Transformiert Wertpapier-Stammdaten aus Staging in Vortabelle
    -- =========================================================================
    PROCEDURE transform_wertpapier (p_batch_id IN NUMBER) AS
        v_anzahl NUMBER := 0;
    BEGIN
        -- Zunaechst vorhandene Vortabellen-Daten fuer diesen Batch loeschen (Idempotenz)
        DELETE FROM VOR_WERTPAPIER WHERE batch_id = p_batch_id;

        INSERT INTO VOR_WERTPAPIER (
            batch_id, isin, wkn, valor, ticker,
            bezeichnung_lang, bezeichnung_kurz,
            wertpapiertyp, asset_klasse, sub_asset_klasse,
            emittent_name, land_iso3, branche, branche_code,
            nominalwert, nominalwaehrung, faelligkeitsdatum,
            kuponrate, kuponfrequenz,
            fondstyp, ausschuettungsart, auflagedatum,
            handelswährung, notizwaehrung, hauptboerse,
            ist_gueltig, validierungsfehler,
            quellsystem, src_geaendert_am
        )
        SELECT
            p_batch_id,
            src.isin,
            src.wkn,
            src.valor,
            src.ticker,
            TRIM(src.bezeichnung_lang),
            TRIM(src.bezeichnung_kurz),
            UPPER(TRIM(src.wertpapiertyp)),
            UPPER(TRIM(src.asset_klasse)),
            UPPER(TRIM(src.sub_asset_klasse)),
            TRIM(src.emittent_name),
            UPPER(TRIM(src.land_emittent)),
            TRIM(src.branche),
            TRIM(src.branche_code),
            src.nominalwert,
            UPPER(TRIM(src.nominalwaehrung)),
            src.fälligkeitsdatum,
            src.kuponrate,
            src.kuponfrequenz,
            UPPER(TRIM(src.fondstyp)),
            UPPER(TRIM(src.ausschuettungsart)),
            src.auflagedatum,
            UPPER(TRIM(src.handelswährung)),
            UPPER(TRIM(src.notizwaehrung)),
            TRIM(src.hauptboerse),
            -- Validierung
            CASE WHEN validate_isin(src.isin) IS NOT NULL THEN 'N'
                 WHEN src.bezeichnung_lang IS NULL         THEN 'N'
                 WHEN src.wertpapiertyp IS NULL            THEN 'N'
                 ELSE 'J'
            END,
            -- Fehlerbeschreibung
            CASE WHEN validate_isin(src.isin) IS NOT NULL
                 THEN 'ISIN ungueltig: ' || validate_isin(src.isin)
                 WHEN src.bezeichnung_lang IS NULL
                 THEN 'Bezeichnung fehlt'
                 WHEN src.wertpapiertyp IS NULL
                 THEN 'Wertpapiertyp fehlt'
            END,
            'WERTPAPIERSTAMMDATEN',
            src.src_geaendert_am
        FROM   STG_WP_STAMMDATEN src
        WHERE  src.batch_id = p_batch_id
        -- Letzten Satz je ISIN pro Batch nehmen (bei mehrfacher Lieferung)
        AND    src.stg_id = (
            SELECT MAX(s2.stg_id)
            FROM   STG_WP_STAMMDATEN s2
            WHERE  s2.isin     = src.isin
            AND    s2.batch_id = src.batch_id
        );

        v_anzahl := SQL%ROWCOUNT;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_wertpapier;


    -- =========================================================================
    -- TRANSFORM_FONDS
    -- Transformiert Fonds-Stammdaten aus Staging in Vortabelle
    -- =========================================================================
    PROCEDURE transform_fonds (p_batch_id IN NUMBER) AS
    BEGIN
        DELETE FROM VOR_FONDS WHERE batch_id = p_batch_id;

        INSERT INTO VOR_FONDS (
            batch_id, fonds_id, isin, wkn,
            fondsname, fondsname_kurz,
            fondstyp, investmentstrategie, asset_klasse,
            fondswaehrung, auflagedatum,
            kag_name, verwahrstelle,
            domizil_land, rechtliche_struktur, ucits_konform,
            status,
            ist_gueltig, validierungsfehler,
            quellsystem, src_geaendert_am
        )
        SELECT
            p_batch_id,
            src.fonds_id,
            src.isin,
            src.wkn,
            TRIM(src.fondsname),
            TRIM(src.fondsname_kurz),
            UPPER(TRIM(src.fondstyp)),
            TRIM(src.investmentstrategie),
            UPPER(TRIM(src.asset_klasse)),
            UPPER(TRIM(src.fondswaehrung)),
            src.auflagedatum,
            TRIM(src.kag_name),
            TRIM(src.verwahrstelle),
            UPPER(TRIM(src.domizil_land)),
            TRIM(src.rechtliche_struktur),
            UPPER(TRIM(src.ucits_konform)),
            UPPER(TRIM(src.status)),
            -- Validierung
            CASE WHEN src.fonds_id IS NULL     THEN 'N'
                 WHEN src.fondsname IS NULL     THEN 'N'
                 WHEN src.fondstyp IS NULL      THEN 'N'
                 WHEN src.fondswaehrung IS NULL THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN src.fonds_id IS NULL     THEN 'Fonds-ID fehlt'
                 WHEN src.fondsname IS NULL     THEN 'Fondsname fehlt'
                 WHEN src.fondstyp IS NULL      THEN 'Fondstyp fehlt'
                 WHEN src.fondswaehrung IS NULL THEN 'Fondswaehrung fehlt'
            END,
            'WERTPAPIERSTAMMDATEN',
            src.src_geaendert_am
        FROM   STG_WP_FONDS_STAMMDATEN src
        WHERE  src.batch_id = p_batch_id
        AND    src.stg_id = (
            SELECT MAX(s2.stg_id)
            FROM   STG_WP_FONDS_STAMMDATEN s2
            WHERE  s2.fonds_id = src.fonds_id
            AND    s2.batch_id = src.batch_id
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_fonds;


    -- =========================================================================
    -- TRANSFORM_WECHSELKURSE
    -- =========================================================================
    PROCEDURE transform_wechselkurse (p_batch_id IN NUMBER) AS
    BEGIN
        DELETE FROM VOR_WECHSELKURS WHERE batch_id = p_batch_id;

        INSERT INTO VOR_WECHSELKURS (
            batch_id, waehrung_von, waehrung_nach,
            kursdatum, kurstyp, kurs,
            ist_gueltig, validierungsfehler
        )
        SELECT
            p_batch_id,
            UPPER(TRIM(src.waehrung_von)),
            UPPER(TRIM(src.waehrung_nach)),
            src.kursdatum,
            UPPER(TRIM(src.kurstyp)),
            src.kurs,
            CASE WHEN src.kurs IS NULL OR src.kurs <= 0 THEN 'N'
                 WHEN src.waehrung_von IS NULL           THEN 'N'
                 WHEN src.waehrung_nach IS NULL          THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN src.kurs IS NULL OR src.kurs <= 0
                 THEN 'Kurs muss groesser als 0 sein'
                 WHEN src.waehrung_von IS NULL THEN 'Quellwaehrung fehlt'
                 WHEN src.waehrung_nach IS NULL THEN 'Zielwaehrung fehlt'
            END
        FROM   STG_KV_WECHSELKURS src
        WHERE  src.batch_id = p_batch_id;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_wechselkurse;


    -- =========================================================================
    -- TRANSFORM_KURSE
    -- Transformiert Marktpreise, ergaenzt EUR-Kurs
    -- =========================================================================
    PROCEDURE transform_kurse (p_batch_id IN NUMBER) AS
    BEGIN
        DELETE FROM VOR_KURS WHERE batch_id = p_batch_id;

        INSERT INTO VOR_KURS (
            batch_id, isin, kursdatum, kurstyp,
            kurs, waehrung,
            boerse, kurs_quelle, kurs_qualitaet,
            kurs_in_eur, wechselkurs_eur,
            ist_gueltig, validierungsfehler,
            src_kurs_id
        )
        SELECT
            p_batch_id,
            src.isin,
            src.kursdatum,
            UPPER(TRIM(src.kurstyp)),
            src.kurs,
            UPPER(TRIM(src.waehrung)),
            src.boerse,
            src.kurs_quelle,
            src.kurs_qualitaet,
            -- EUR-Umrechnung
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN src.kurs
                ELSE src.kurs * get_eur_wechselkurs(
                                    UPPER(TRIM(src.waehrung)),
                                    src.kursdatum,
                                    p_batch_id)
            END,
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN 1.0
                ELSE get_eur_wechselkurs(
                         UPPER(TRIM(src.waehrung)),
                         src.kursdatum,
                         p_batch_id)
            END,
            -- Validierung
            CASE WHEN validate_isin(src.isin) IS NOT NULL THEN 'N'
                 WHEN src.kurs IS NULL OR src.kurs < 0    THEN 'N'
                 WHEN src.waehrung IS NULL                THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN validate_isin(src.isin) IS NOT NULL
                 THEN 'Ungueltige ISIN: ' || validate_isin(src.isin)
                 WHEN src.kurs IS NULL OR src.kurs < 0
                 THEN 'Kurs ungueltig (NULL oder negativ)'
                 WHEN src.waehrung IS NULL THEN 'Waehrung fehlt'
            END,
            src.src_kurs_id
        FROM   STG_KV_KURS src
        WHERE  src.batch_id = p_batch_id;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_kurse;


    -- =========================================================================
    -- TRANSFORM_BESTAENDE
    -- Transformiert Fondsbestaende, ergaenzt EUR-Werte und Portfoliogewicht
    -- =========================================================================
    PROCEDURE transform_bestaende (p_batch_id IN NUMBER) AS
        v_gesamt_fondsverm NUMBER;
    BEGIN
        DELETE FROM VOR_BESTAND WHERE batch_id = p_batch_id;

        -- Schritt 1: Bestaende mit EUR-Werten einfuegen
        INSERT INTO VOR_BESTAND (
            batch_id, fonds_id, isin, bewertungsdatum,
            nominale, stueckzahl,
            einstandskurs, einstandswert,
            aktueller_kurs, marktwert,
            marktwert_fondswährung, wechselkurs,
            marktwert_eur, wechselkurs_eur,
            portfoliogewicht,
            waehrung_papier, waehrung_fonds,
            depot_nr, depotstelle,
            ist_gueltig, validierungsfehler,
            stg_id
        )
        SELECT
            p_batch_id,
            src.fonds_id,
            src.isin,
            src.bewertungsdatum,
            src.nominale,
            src.stueckzahl,
            src.einstandskurs,
            src.einstandswert,
            src.aktueller_kurs,
            src.marktwert,
            src.marktwert_fondswährung,
            src.wechselkurs,
            -- EUR-Marktwert berechnen
            CASE UPPER(TRIM(src.waehrung_fonds))
                WHEN 'EUR' THEN src.marktwert_fondswährung
                ELSE src.marktwert_fondswährung * get_eur_wechselkurs(
                                                      UPPER(TRIM(src.waehrung_fonds)),
                                                      src.bewertungsdatum,
                                                      p_batch_id)
            END,
            CASE UPPER(TRIM(src.waehrung_fonds))
                WHEN 'EUR' THEN 1.0
                ELSE get_eur_wechselkurs(
                         UPPER(TRIM(src.waehrung_fonds)),
                         src.bewertungsdatum,
                         p_batch_id)
            END,
            0, -- Portfoliogewicht wird in Schritt 2 berechnet
            UPPER(TRIM(src.waehrung_papier)),
            UPPER(TRIM(src.waehrung_fonds)),
            src.depot_nr,
            src.depotstelle,
            -- Validierung
            CASE WHEN src.fonds_id IS NULL        THEN 'N'
                 WHEN validate_isin(src.isin) IS NOT NULL THEN 'N'
                 WHEN src.bewertungsdatum IS NULL  THEN 'N'
                 WHEN src.marktwert IS NULL        THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN src.fonds_id IS NULL THEN 'Fonds-ID fehlt'
                 WHEN validate_isin(src.isin) IS NOT NULL
                 THEN 'Ungueltige ISIN: ' || validate_isin(src.isin)
                 WHEN src.bewertungsdatum IS NULL THEN 'Bewertungsdatum fehlt'
                 WHEN src.marktwert IS NULL THEN 'Marktwert fehlt'
            END,
            src.stg_id
        FROM   STG_FB_BESTAND src
        WHERE  src.batch_id = p_batch_id
        AND    src.stg_id = (
            SELECT MAX(s2.stg_id)
            FROM   STG_FB_BESTAND s2
            WHERE  s2.fonds_id        = src.fonds_id
            AND    s2.isin            = src.isin
            AND    s2.bewertungsdatum = src.bewertungsdatum
            AND    s2.batch_id        = src.batch_id
        );

        -- Schritt 2: Portfoliogewicht berechnen (Anteil am gesamten Fondsvermögen)
        UPDATE VOR_BESTAND vb
        SET    portfoliogewicht = (
            SELECT CASE WHEN SUM(vb2.marktwert_fondswährung) = 0 THEN 0
                        ELSE ROUND(vb.marktwert_fondswährung
                                   / SUM(vb2.marktwert_fondswährung) * 100, 6)
                   END
            FROM   VOR_BESTAND vb2
            WHERE  vb2.fonds_id       = vb.fonds_id
            AND    vb2.bewertungsdatum = vb.bewertungsdatum
            AND    vb2.batch_id        = vb.batch_id
            AND    vb2.ist_gueltig     = 'J'
        )
        WHERE  vb.batch_id = p_batch_id
        AND    vb.ist_gueltig = 'J';

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_bestaende;


    -- =========================================================================
    -- TRANSFORM_TRANSAKTIONEN
    -- =========================================================================
    PROCEDURE transform_transaktionen (p_batch_id IN NUMBER) AS
    BEGIN
        DELETE FROM VOR_TRANSAKTION WHERE batch_id = p_batch_id;

        INSERT INTO VOR_TRANSAKTION (
            batch_id, transaktions_id, fonds_id, isin,
            transaktionstyp, transaktionstyp_normiert,
            handelsdatum, valutatdatum, buchungsdatum,
            stueckzahl, nominale,
            kurs, transaktionswert, provision, stueckzinsen, gesamtbetrag,
            gesamtbetrag_eur, wechselkurs_eur,
            waehrung, kontrahent, depot_nr,
            ist_gueltig, validierungsfehler,
            stg_id
        )
        SELECT
            p_batch_id,
            src.transaktions_id,
            src.fonds_id,
            src.isin,
            UPPER(TRIM(src.transaktionstyp)),
            normiere_transaktionstyp(src.transaktionstyp),
            src.handelsdatum,
            src.valutatdatum,
            NVL(src.buchungsdatum, src.valutatdatum),
            src.stueckzahl,
            src.nominale,
            src.kurs,
            src.transaktionswert,
            NVL(src.provision, 0),
            NVL(src.stueckzinsen, 0),
            NVL(src.gesamtbetrag,
                NVL(src.transaktionswert, 0) + NVL(src.provision, 0)
                + NVL(src.stueckzinsen, 0)),
            -- EUR-Umrechnung des Gesamtbetrags
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN NVL(src.gesamtbetrag,
                                    NVL(src.transaktionswert,0) + NVL(src.provision,0))
                ELSE NVL(src.gesamtbetrag,
                         NVL(src.transaktionswert,0) + NVL(src.provision,0))
                     * get_eur_wechselkurs(UPPER(TRIM(src.waehrung)),
                                           src.handelsdatum,
                                           p_batch_id)
            END,
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN 1.0
                ELSE get_eur_wechselkurs(UPPER(TRIM(src.waehrung)),
                                         src.handelsdatum,
                                         p_batch_id)
            END,
            UPPER(TRIM(src.waehrung)),
            src.kontrahent,
            src.depot_nr,
            -- Validierung
            CASE WHEN src.transaktions_id IS NULL          THEN 'N'
                 WHEN src.fonds_id IS NULL                 THEN 'N'
                 WHEN validate_isin(src.isin) IS NOT NULL  THEN 'N'
                 WHEN src.handelsdatum IS NULL             THEN 'N'
                 WHEN src.transaktionstyp IS NULL          THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN src.transaktions_id IS NULL THEN 'Transaktions-ID fehlt'
                 WHEN src.fonds_id IS NULL THEN 'Fonds-ID fehlt'
                 WHEN validate_isin(src.isin) IS NOT NULL
                 THEN 'Ungueltige ISIN: ' || validate_isin(src.isin)
                 WHEN src.handelsdatum IS NULL THEN 'Handelsdatum fehlt'
                 WHEN src.transaktionstyp IS NULL THEN 'Transaktionstyp fehlt'
            END,
            src.stg_id
        FROM   STG_FB_TRANSAKTION src
        WHERE  src.batch_id = p_batch_id;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_transaktionen;


    -- =========================================================================
    -- TRANSFORM_FONDSBEWEGUNGEN
    -- =========================================================================
    PROCEDURE transform_fondsbewegungen (p_batch_id IN NUMBER) AS
    BEGIN
        DELETE FROM VOR_FONDSBEWEGUNG WHERE batch_id = p_batch_id;

        INSERT INTO VOR_FONDSBEWEGUNG (
            batch_id, bewegungs_id, fonds_id,
            bewegungstyp, bewegungstyp_normiert,
            handelsdatum, valutatdatum, buchungsdatum,
            anteile, anteilswert_nav,
            bruttobetrag, nettobetrag, ausgabeaufschlag,
            nettobetrag_eur, wechselkurs_eur,
            depot_nr, anleger_id, waehrung,
            ist_gueltig, validierungsfehler,
            stg_id
        )
        SELECT
            p_batch_id,
            src.bewegungs_id,
            src.fonds_id,
            UPPER(TRIM(src.bewegungstyp)),
            normiere_bewegungstyp(src.bewegungstyp),
            src.handelsdatum,
            src.valutatdatum,
            NVL(src.buchungsdatum, src.valutatdatum),
            src.anteile,
            src.anteilswert_nav,
            src.bruttobetrag,
            NVL(src.nettobetrag, src.bruttobetrag),
            NVL(src.ausgabeaufschlag, 0),
            -- EUR-Umrechnung
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN NVL(src.nettobetrag, src.bruttobetrag)
                ELSE NVL(src.nettobetrag, src.bruttobetrag)
                     * get_eur_wechselkurs(UPPER(TRIM(src.waehrung)),
                                           src.handelsdatum,
                                           p_batch_id)
            END,
            CASE UPPER(TRIM(src.waehrung))
                WHEN 'EUR' THEN 1.0
                ELSE get_eur_wechselkurs(UPPER(TRIM(src.waehrung)),
                                         src.handelsdatum,
                                         p_batch_id)
            END,
            src.depot_nr,
            src.anleger_id,
            UPPER(TRIM(src.waehrung)),
            -- Validierung
            CASE WHEN src.bewegungs_id IS NULL  THEN 'N'
                 WHEN src.fonds_id IS NULL       THEN 'N'
                 WHEN src.handelsdatum IS NULL   THEN 'N'
                 WHEN src.anteile IS NULL        THEN 'N'
                 ELSE 'J'
            END,
            CASE WHEN src.bewegungs_id IS NULL THEN 'Bewegungs-ID fehlt'
                 WHEN src.fonds_id IS NULL THEN 'Fonds-ID fehlt'
                 WHEN src.handelsdatum IS NULL THEN 'Handelsdatum fehlt'
                 WHEN src.anteile IS NULL THEN 'Anteile fehlen'
            END,
            src.stg_id
        FROM   STG_FB_FONDSBEWEGUNG src
        WHERE  src.batch_id = p_batch_id;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END transform_fondsbewegungen;


    -- =========================================================================
    -- RUN_TRANSFORMATION
    -- Hauptprozedur: Fuehrt alle Transformationen in der richtigen Reihenfolge aus
    -- =========================================================================
    PROCEDURE run_transformation (p_batch_id IN NUMBER) AS
    BEGIN
        -- 1. Stammdaten (Basis fuer Dimensionsbeladung)
        transform_wertpapier(p_batch_id);
        transform_fonds(p_batch_id);

        -- 2. Kursdaten (Wechselkurse zuerst, dann Wertpapierkurse)
        transform_wechselkurse(p_batch_id);
        transform_kurse(p_batch_id);

        -- 3. Bewegungsdaten
        transform_bestaende(p_batch_id);
        transform_transaktionen(p_batch_id);
        transform_fondsbewegungen(p_batch_id);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END run_transformation;

END PKG_ETL_VORTABELLEN;
/

-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- Dimensionstabellen (Layer 3: Stammdaten fuer das DWH-Sternschema)
-- =============================================================================
-- Beschreibung:
--   Dimensionstabellen enthalten die beschreibenden Stammdaten fuer das
--   Data Warehouse. Sie sind nach dem Slowly Changing Dimension (SCD)
--   Typ 2 Ansatz modelliert, um Historisierung zu ermoeglichen.
--
-- SCD Typ 2 Felder:
--   gueltig_von  - Datum, ab dem der Datensatz gueltig ist
--   gueltig_bis  - Datum, bis zu dem der Datensatz gueltig ist (9999-12-31 = aktuell)
--   ist_aktuell  - Flag fuer aktuellen Datensatz (J/N)
--   dwh_version  - Versionsnummer des Datensatzes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_ZEIT: Zeitdimension (vorberechnet fuer schnelle Abfragen)
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_ZEIT (
    zeit_id             NUMBER          NOT NULL,   -- Format: YYYYMMDD
    datum               DATE            NOT NULL,
    -- Tagesattribute
    tag_nr              NUMBER(2)       NOT NULL,   -- 1-31
    wochentag_nr        NUMBER(1)       NOT NULL,   -- 1=Mo, 7=So
    wochentag_name      VARCHAR2(20)    NOT NULL,
    wochentag_kuerzel   VARCHAR2(3)     NOT NULL,
    ist_wochenende      VARCHAR2(1)     NOT NULL CHECK (ist_wochenende IN ('J','N')),
    ist_bankarbeitstag  VARCHAR2(1)     DEFAULT 'J' CHECK (ist_bankarbeitstag IN ('J','N')),
    ist_feiertag        VARCHAR2(1)     DEFAULT 'N' CHECK (ist_feiertag IN ('J','N')),
    feiertag_name       VARCHAR2(100),
    -- Wochenattribute
    woche_im_jahr       NUMBER(2)       NOT NULL,
    -- Monatsattribute
    monat_nr            NUMBER(2)       NOT NULL,   -- 1-12
    monat_name          VARCHAR2(20)    NOT NULL,
    monat_kuerzel       VARCHAR2(3)     NOT NULL,
    quartal_nr          NUMBER(1)       NOT NULL,   -- 1-4
    quartal_name        VARCHAR2(10)    NOT NULL,
    -- Jahresattribute
    jahr                NUMBER(4)       NOT NULL,
    -- Geschaeftsjahresattribute (anpassbar)
    gj_monat_nr         NUMBER(2),
    gj_quartal_nr       NUMBER(1),
    gj_jahr             NUMBER(4),
    -- Relative Zeitattribute (zum aktuellen Datum)
    tage_bis_heute      NUMBER,
    monate_bis_heute    NUMBER,
    CONSTRAINT PK_DIM_ZEIT PRIMARY KEY (zeit_id)
);

CREATE UNIQUE INDEX UIX_DIM_ZEIT_DATUM ON DIM_ZEIT (datum);

COMMENT ON TABLE  DIM_ZEIT                  IS 'Zeitdimension: Vorberechnete Kalenderattribute fuer alle relevanten Datumsangaben';
COMMENT ON COLUMN DIM_ZEIT.zeit_id          IS 'Surrogatschluessel im Format YYYYMMDD (z.B. 20240115)';
COMMENT ON COLUMN DIM_ZEIT.ist_bankarbeitstag IS 'Bankarbeitstag gem. Zielmarktkalender (TARGET2)';

-- Prozedur zum Befuellen der Zeitdimension
CREATE OR REPLACE PROCEDURE PROC_FILL_DIM_ZEIT (
    p_von_datum  IN DATE DEFAULT DATE '2000-01-01',
    p_bis_datum  IN DATE DEFAULT DATE '2040-12-31'
) AS
    v_datum      DATE;
    v_monat_de   VARCHAR2(20);
    v_wotag_de   VARCHAR2(20);
    v_wotag_kz   VARCHAR2(3);
BEGIN
    v_datum := p_von_datum;
    WHILE v_datum <= p_bis_datum LOOP
        -- Monatsnamen (Deutsch)
        v_monat_de := CASE TO_NUMBER(TO_CHAR(v_datum, 'MM'))
            WHEN 1  THEN 'Januar'   WHEN 2  THEN 'Februar'
            WHEN 3  THEN 'März'     WHEN 4  THEN 'April'
            WHEN 5  THEN 'Mai'      WHEN 6  THEN 'Juni'
            WHEN 7  THEN 'Juli'     WHEN 8  THEN 'August'
            WHEN 9  THEN 'September' WHEN 10 THEN 'Oktober'
            WHEN 11 THEN 'November' WHEN 12 THEN 'Dezember'
        END;
        -- Wochentagsnamen (Deutsch, ISO: 1=Mo, 7=So)
        v_wotag_de := CASE TO_NUMBER(TO_CHAR(v_datum, 'D', 'NLS_DATE_LANGUAGE=AMERICAN'))
            WHEN 1 THEN 'Montag'     WHEN 2 THEN 'Dienstag'
            WHEN 3 THEN 'Mittwoch'   WHEN 4 THEN 'Donnerstag'
            WHEN 5 THEN 'Freitag'    WHEN 6 THEN 'Samstag'
            WHEN 7 THEN 'Sonntag'
        END;
        v_wotag_kz := SUBSTR(v_wotag_de, 1, 2);

        MERGE INTO DIM_ZEIT d
        USING (SELECT TO_NUMBER(TO_CHAR(v_datum,'YYYYMMDD')) AS xid FROM DUAL) s
        ON (d.zeit_id = s.xid)
        WHEN NOT MATCHED THEN INSERT (
            zeit_id, datum, tag_nr, wochentag_nr, wochentag_name, wochentag_kuerzel,
            ist_wochenende, woche_im_jahr, monat_nr, monat_name, monat_kuerzel,
            quartal_nr, quartal_name, jahr
        ) VALUES (
            TO_NUMBER(TO_CHAR(v_datum,'YYYYMMDD')),
            v_datum,
            TO_NUMBER(TO_CHAR(v_datum,'DD')),
            TO_NUMBER(TO_CHAR(v_datum,'D','NLS_DATE_LANGUAGE=AMERICAN')),
            v_wotag_de,
            v_wotag_kz,
            CASE WHEN TO_NUMBER(TO_CHAR(v_datum,'D','NLS_DATE_LANGUAGE=AMERICAN')) IN (6,7)
                 THEN 'J' ELSE 'N' END,
            TO_NUMBER(TO_CHAR(v_datum,'IW')),
            TO_NUMBER(TO_CHAR(v_datum,'MM')),
            v_monat_de,
            SUBSTR(v_monat_de,1,3),
            TO_NUMBER(TO_CHAR(v_datum,'Q')),
            'Q' || TO_CHAR(v_datum,'Q') || '/' || TO_CHAR(v_datum,'YYYY'),
            TO_NUMBER(TO_CHAR(v_datum,'YYYY'))
        );
        v_datum := v_datum + 1;
    END LOOP;
    COMMIT;
END PROC_FILL_DIM_ZEIT;
/


-- -----------------------------------------------------------------------------
-- DIM_WAEHRUNG: Waehrungs-Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_WAEHRUNG (
    waehrung_id         NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Fachliche Attribute
    iso_code            VARCHAR2(3)     NOT NULL,
    iso_code_numerisch  VARCHAR2(3),
    name_de             VARCHAR2(100)   NOT NULL,
    name_en             VARCHAR2(100),
    symbol              VARCHAR2(5),
    dezimalstellen      NUMBER(1)       DEFAULT 2,
    ist_euro_waehrung   VARCHAR2(1)     DEFAULT 'N' CHECK (ist_euro_waehrung IN ('J','N')),
    land_iso3           VARCHAR2(3),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    geaendert_am        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_DIM_WAEHRUNG PRIMARY KEY (waehrung_id)
);

CREATE INDEX IDX_DIM_WRG_ISO       ON DIM_WAEHRUNG (iso_code);
CREATE INDEX IDX_DIM_WRG_AKTUELL   ON DIM_WAEHRUNG (iso_code, ist_aktuell);

COMMENT ON TABLE  DIM_WAEHRUNG              IS 'Dimension: Waehrungen (ISO 4217)';
COMMENT ON COLUMN DIM_WAEHRUNG.ist_aktuell  IS 'Aktueller SCD-Datensatz (J=aktuell, N=historisch)';

-- Basis-Waehrungsdaten einfuegen
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'EUR', '978', 'Euro',           'Euro',           '€',  2, 'J');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'USD', '840', 'US-Dollar',      'US Dollar',      '$',  2, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'CHF', '756', 'Schweizer Franken','Swiss Franc',  'Fr', 2, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'GBP', '826', 'Britisches Pfund','British Pound', '£',  2, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'JPY', '392', 'Japanischer Yen','Japanese Yen',  '¥',  0, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'SEK', '752', 'Schwedische Krone','Swedish Krona','kr', 2, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'NOK', '578', 'Norwegische Krone','Norwegian Krone','kr',2, 'N');
INSERT INTO DIM_WAEHRUNG (gueltig_von, iso_code, iso_code_numerisch, name_de, name_en, symbol, dezimalstellen, ist_euro_waehrung)
VALUES (DATE '2000-01-01', 'DKK', '208', 'Dänische Krone', 'Danish Krone',  'kr', 2, 'N');
COMMIT;


-- -----------------------------------------------------------------------------
-- DIM_LAND: Laender-Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_LAND (
    land_id             NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Fachliche Attribute
    iso3_code           VARCHAR2(3)     NOT NULL,  -- ISO 3166-1 Alpha-3
    iso2_code           VARCHAR2(2),               -- ISO 3166-1 Alpha-2
    iso_numerisch       VARCHAR2(3),
    name_de             VARCHAR2(200)   NOT NULL,
    name_en             VARCHAR2(200),
    region              VARCHAR2(100),
    sub_region          VARCHAR2(100),
    waehrung_iso3       VARCHAR2(3),
    ist_eu_mitglied     VARCHAR2(1)     DEFAULT 'N' CHECK (ist_eu_mitglied IN ('J','N')),
    ist_eurozone        VARCHAR2(1)     DEFAULT 'N' CHECK (ist_eurozone IN ('J','N')),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    geaendert_am        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_DIM_LAND PRIMARY KEY (land_id)
);

CREATE INDEX IDX_DIM_LAND_ISO3     ON DIM_LAND (iso3_code);
CREATE INDEX IDX_DIM_LAND_AKTUELL  ON DIM_LAND (iso3_code, ist_aktuell);

COMMENT ON TABLE DIM_LAND IS 'Dimension: Laender (ISO 3166-1)';

-- Wichtige Laender einfuegen
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'DEU', 'DE', 'Deutschland',       'Germany',        'Europa',   'EUR', 'J', 'J');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'AUT', 'AT', 'Österreich',        'Austria',        'Europa',   'EUR', 'J', 'J');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'CHE', 'CH', 'Schweiz',           'Switzerland',    'Europa',   'CHF', 'N', 'N');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'USA', 'US', 'Vereinigte Staaten','United States',  'Amerika',  'USD', 'N', 'N');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'GBR', 'GB', 'Vereinigtes Königreich','United Kingdom','Europa','GBP', 'N', 'N');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'JPN', 'JP', 'Japan',             'Japan',          'Asien',    'JPY', 'N', 'N');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'LUX', 'LU', 'Luxemburg',         'Luxembourg',     'Europa',   'EUR', 'J', 'J');
INSERT INTO DIM_LAND (gueltig_von, iso3_code, iso2_code, name_de, name_en, region, waehrung_iso3, ist_eu_mitglied, ist_eurozone)
VALUES (DATE '2000-01-01', 'IRL', 'IE', 'Irland',            'Ireland',        'Europa',   'EUR', 'J', 'J');
COMMIT;


-- -----------------------------------------------------------------------------
-- DIM_BRANCHE: Branchen-Dimension (GICS-basiert)
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_BRANCHE (
    branche_id          NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Fachliche Attribute (GICS-Hierarchie)
    gics_sektor_code    VARCHAR2(10),
    gics_sektor_name    VARCHAR2(100),
    gics_branche_code   VARCHAR2(10),
    gics_branche_name   VARCHAR2(100),
    gics_sub_branche_code VARCHAR2(10),
    gics_sub_branche_name VARCHAR2(100),
    -- Alternative Klassifikation
    sic_code            VARCHAR2(10),
    sic_name            VARCHAR2(200),
    -- Eigene Klassifikation
    branche_intern_code VARCHAR2(20),
    branche_intern_name VARCHAR2(200),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_DIM_BRANCHE PRIMARY KEY (branche_id)
);

CREATE INDEX IDX_DIM_BRN_GICS  ON DIM_BRANCHE (gics_sektor_code, ist_aktuell);
CREATE INDEX IDX_DIM_BRN_INTERN ON DIM_BRANCHE (branche_intern_code, ist_aktuell);

COMMENT ON TABLE DIM_BRANCHE IS 'Dimension: Branchen und Sektoren (GICS-Klassifikation)';

-- GICS-Sektoren einfuegen
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '10', 'Energie',                    'ENERGIE',   'Energie');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '15', 'Rohstoffe',                  'ROHSTOFF',  'Rohstoffe');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '20', 'Industrie',                  'INDUSTRIE', 'Industrie');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '25', 'Nicht-Basiskonsumgüter',     'KONSUM_ZY', 'Nicht-Basiskonsumgüter');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '30', 'Basiskonsumgüter',           'KONSUM_ST', 'Basiskonsumgüter');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '35', 'Gesundheitswesen',           'GESUNDH',   'Gesundheitswesen');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '40', 'Finanzwesen',                'FINANZ',    'Finanzwesen');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '45', 'Informationstechnologie',    'IT',        'Informationstechnologie');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '50', 'Telekommunikation',          'TELEKOM',   'Telekommunikation');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '55', 'Versorger',                  'VERSORGER', 'Versorger');
INSERT INTO DIM_BRANCHE (gueltig_von, gics_sektor_code, gics_sektor_name, branche_intern_code, branche_intern_name)
VALUES (DATE '2000-01-01', '60', 'Immobilien',                 'IMMOBILIEN','Immobilien');
COMMIT;


-- -----------------------------------------------------------------------------
-- DIM_WERTPAPIER: Wertpapier-Dimension (SCD Typ 2)
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_WERTPAPIER (
    wertpapier_sk       NUMBER          GENERATED ALWAYS AS IDENTITY,  -- Surrogatschluessel
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Natuerliche Schluessel
    isin                VARCHAR2(12)    NOT NULL,
    wkn                 VARCHAR2(6),
    valor               VARCHAR2(20),
    ticker              VARCHAR2(20),
    -- Bezeichnungen
    bezeichnung_lang    VARCHAR2(500)   NOT NULL,
    bezeichnung_kurz    VARCHAR2(100),
    -- Klassifikation
    wertpapiertyp       VARCHAR2(50)    NOT NULL,
    asset_klasse        VARCHAR2(50),
    sub_asset_klasse    VARCHAR2(50),
    -- Emittent
    emittent_name       VARCHAR2(500),
    -- FK zu Dimensionen
    land_id             NUMBER          REFERENCES DIM_LAND(land_id),
    branche_id          NUMBER          REFERENCES DIM_BRANCHE(branche_id),
    waehrung_id         NUMBER          REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Anleihe-Attribute
    nominalwert         NUMBER(20,4),
    nominalwaehrung     VARCHAR2(3),
    faelligkeitsdatum   DATE,
    kuponrate           NUMBER(10,6),
    kuponfrequenz       VARCHAR2(20),
    -- Fonds-Attribute
    fondstyp            VARCHAR2(50),
    ausschuettungsart   VARCHAR2(20),
    auflagedatum        DATE,
    -- Handelsinformationen
    hauptboerse         VARCHAR2(50),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    geaendert_am        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id_erstellt   NUMBER,
    batch_id_geaendert  NUMBER,
    CONSTRAINT PK_DIM_WERTPAPIER PRIMARY KEY (wertpapier_sk)
);

CREATE INDEX IDX_DIM_WP_ISIN       ON DIM_WERTPAPIER (isin, ist_aktuell);
CREATE INDEX IDX_DIM_WP_AKTUELL    ON DIM_WERTPAPIER (ist_aktuell, isin);
CREATE INDEX IDX_DIM_WP_GUELT      ON DIM_WERTPAPIER (isin, gueltig_von, gueltig_bis);

COMMENT ON TABLE  DIM_WERTPAPIER              IS 'Dimension: Wertpapier-Stammdaten mit SCD Typ 2 Historisierung';
COMMENT ON COLUMN DIM_WERTPAPIER.wertpapier_sk IS 'Surrogatschluessel (technischer DWH-Schluessel)';
COMMENT ON COLUMN DIM_WERTPAPIER.isin          IS 'ISIN als natuerlicher (fachlicher) Schluessel';
COMMENT ON COLUMN DIM_WERTPAPIER.ist_aktuell   IS 'J = aktuell gueltiger Stammsatz, N = historischer Stammsatz';


-- -----------------------------------------------------------------------------
-- DIM_FONDS: Fonds-Dimension (SCD Typ 2)
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_FONDS (
    fonds_sk            NUMBER          GENERATED ALWAYS AS IDENTITY,  -- Surrogatschluessel
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Natuerliche Schluessel
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12),
    wkn                 VARCHAR2(6),
    -- Bezeichnungen
    fondsname           VARCHAR2(500)   NOT NULL,
    fondsname_kurz      VARCHAR2(100),
    -- Klassifikation
    fondstyp            VARCHAR2(50)    NOT NULL,
    investmentstrategie VARCHAR2(200),
    asset_klasse        VARCHAR2(50),
    -- FK zu Dimensionen
    fondswaehrung_id    NUMBER          REFERENCES DIM_WAEHRUNG(waehrung_id),
    domizil_land_id     NUMBER          REFERENCES DIM_LAND(land_id),
    -- Kapital/Daten
    auflagedatum        DATE,
    -- Verwaltungsgesellschaft
    kag_name            VARCHAR2(500),
    verwahrstelle       VARCHAR2(500),
    -- Rechtliches
    rechtliche_struktur VARCHAR2(100),
    ucits_konform       VARCHAR2(1),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    geaendert_am        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id_erstellt   NUMBER,
    batch_id_geaendert  NUMBER,
    CONSTRAINT PK_DIM_FONDS PRIMARY KEY (fonds_sk)
);

CREATE INDEX IDX_DIM_FONDS_ID      ON DIM_FONDS (fonds_id, ist_aktuell);
CREATE INDEX IDX_DIM_FONDS_AKTUELL ON DIM_FONDS (ist_aktuell, fonds_id);

COMMENT ON TABLE  DIM_FONDS            IS 'Dimension: Fonds-Stammdaten mit SCD Typ 2 Historisierung';
COMMENT ON COLUMN DIM_FONDS.fonds_sk   IS 'Surrogatschluessel (technischer DWH-Schluessel)';


-- -----------------------------------------------------------------------------
-- DIM_DEPOT: Depot-Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE DIM_DEPOT (
    depot_sk            NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- SCD Typ 2 Felder
    gueltig_von         DATE            NOT NULL,
    gueltig_bis         DATE            DEFAULT DATE '9999-12-31' NOT NULL,
    ist_aktuell         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_aktuell IN ('J','N')),
    dwh_version         NUMBER          DEFAULT 1 NOT NULL,
    -- Natuerlicher Schluessel
    depot_nr            VARCHAR2(50)    NOT NULL,
    -- Attribute
    depotstelle         VARCHAR2(200),
    depot_typ           VARCHAR2(50),
    inhaber_name        VARCHAR2(500),
    inhaber_id          VARCHAR2(100),
    waehrung_id         NUMBER          REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    geaendert_am        TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_DIM_DEPOT PRIMARY KEY (depot_sk)
);

CREATE INDEX IDX_DIM_DEPOT_NR ON DIM_DEPOT (depot_nr, ist_aktuell);

COMMENT ON TABLE DIM_DEPOT IS 'Dimension: Depot-Stammdaten';

COMMIT;

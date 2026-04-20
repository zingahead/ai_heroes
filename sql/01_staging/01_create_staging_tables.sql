-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- Staging-Tabellen (Layer 1: 1:1 Abbild der Quelldaten)
-- =============================================================================
-- Beschreibung:
--   Staging-Tabellen nehmen die Rohdaten aus den Quellsystemen 1:1 auf.
--   Jede Lieferung wird mit einem Lade-Timestamp und einer Batch-ID
--   versehen, um die Nachvollziehbarkeit zu gewaehrleisten.
--
-- Quellsysteme:
--   1. Fondsbuchhaltung  -> Bestaende, Transaktionen, Fondsbewegungen
--   2. Kursversorgung    -> Marktpreise/Kurse
--   3. Wertpapierstammdaten -> Stammdaten der Wertpapiere
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Hilfsobjekte fuer Staging
-- -----------------------------------------------------------------------------

-- Sequenz fuer Batch-IDs (eindeutige Kennung je Ladelauf)
CREATE SEQUENCE SEQ_STG_BATCH_ID
  START WITH 1
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

-- Log-Tabelle fuer Staging-Ladelaeufe
CREATE TABLE STG_LADELOG (
    log_id              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id            NUMBER          NOT NULL,
    quellsystem         VARCHAR2(100)   NOT NULL,
    tabelle             VARCHAR2(100)   NOT NULL,
    ladestart           TIMESTAMP       NOT NULL,
    ladeende            TIMESTAMP,
    anzahl_saetze       NUMBER          DEFAULT 0,
    status              VARCHAR2(20)    DEFAULT 'GESTARTET'
                            CHECK (status IN ('GESTARTET','ERFOLGREICH','FEHLER')),
    fehlermeldung       VARCHAR2(4000),
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

COMMENT ON TABLE  STG_LADELOG                IS 'Protokoll aller Staging-Ladelaeufe';
COMMENT ON COLUMN STG_LADELOG.batch_id       IS 'Eindeutige ID je Ladelauf (aus SEQ_STG_BATCH_ID)';
COMMENT ON COLUMN STG_LADELOG.quellsystem    IS 'Name des Quellsystems (z.B. FONDSBUCHHALTUNG)';
COMMENT ON COLUMN STG_LADELOG.tabelle        IS 'Name der befuellten Staging-Tabelle';
COMMENT ON COLUMN STG_LADELOG.status         IS 'Ladestatus: GESTARTET, ERFOLGREICH, FEHLER';

-- -----------------------------------------------------------------------------
-- 1. Quellsystem: FONDSBUCHHALTUNG
-- -----------------------------------------------------------------------------

-- STG_FB_BESTAND: Tagesaktuelle Fondsbestaende aus der Fondsbuchhaltung
CREATE TABLE STG_FB_BESTAND (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12)    NOT NULL,
    bewertungsdatum     DATE            NOT NULL,
    -- Mengendaten
    nominale            NUMBER(20,6),
    stueckzahl          NUMBER(20,6),
    -- Wertdaten (in Fondswährung)
    einstandskurs       NUMBER(20,6),
    einstandswert       NUMBER(20,2),
    aktueller_kurs      NUMBER(20,6),
    marktwert           NUMBER(20,2),
    -- Waehrung
    waehrung_fonds      VARCHAR2(3),
    waehrung_papier     VARCHAR2(3),
    wechselkurs         NUMBER(15,8),
    marktwert_fondswährung NUMBER(20,2),
    -- Depotinfos
    depot_nr            VARCHAR2(50),
    depotstelle         VARCHAR2(100),
    -- Metadaten Quelle
    src_erstellt_am     DATE,
    src_geaendert_am    DATE,
    CONSTRAINT PK_STG_FB_BESTAND PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_FB_BEST_BATCH   ON STG_FB_BESTAND (batch_id);
CREATE INDEX IDX_STG_FB_BEST_KEY     ON STG_FB_BESTAND (fonds_id, isin, bewertungsdatum);

COMMENT ON TABLE  STG_FB_BESTAND                      IS 'Staging: Fondsbestaende aus Fondsbuchhaltung (1:1 Quelldaten)';
COMMENT ON COLUMN STG_FB_BESTAND.batch_id             IS 'Batch-ID des Ladelaufs';
COMMENT ON COLUMN STG_FB_BESTAND.isin                 IS 'ISIN des Wertpapiers (12-stellig)';
COMMENT ON COLUMN STG_FB_BESTAND.nominale             IS 'Nominale bei Anleihen (Nennwert)';
COMMENT ON COLUMN STG_FB_BESTAND.stueckzahl           IS 'Stueckzahl bei Aktien/Fonds';
COMMENT ON COLUMN STG_FB_BESTAND.einstandswert        IS 'Historischer Einstandswert in Papierwaehrung';
COMMENT ON COLUMN STG_FB_BESTAND.marktwert            IS 'Aktueller Marktwert in Papierwaehrung';


-- STG_FB_TRANSAKTION: Wertpapiertransaktionen (Kaeufe/Verkaeufe)
CREATE TABLE STG_FB_TRANSAKTION (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    transaktions_id     VARCHAR2(100)   NOT NULL,
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12)    NOT NULL,
    -- Transaktionsdetails
    transaktionstyp     VARCHAR2(20)    NOT NULL,   -- KAUF, VERKAUF, TAUSCH, etc.
    handelsdatum        DATE            NOT NULL,
    valutatdatum        DATE,
    buchungsdatum       DATE,
    -- Mengen- und Wertdaten
    stueckzahl          NUMBER(20,6),
    nominale            NUMBER(20,6),
    kurs                NUMBER(20,6),
    kursdatum           DATE,
    transaktionswert    NUMBER(20,2),
    provision           NUMBER(20,2),
    stueckzinsen        NUMBER(20,2),
    gesamtbetrag        NUMBER(20,2),
    -- Waehrung
    waehrung            VARCHAR2(3),
    waehrung_fonds      VARCHAR2(3),
    wechselkurs         NUMBER(15,8),
    gesamtbetrag_fondswährung NUMBER(20,2),
    -- Gegenpartei
    kontrahent          VARCHAR2(200),
    depot_nr            VARCHAR2(50),
    -- Metadaten Quelle
    src_transaktions_nr VARCHAR2(100),
    src_status          VARCHAR2(50),
    src_erstellt_am     DATE,
    src_geaendert_am    DATE,
    CONSTRAINT PK_STG_FB_TRANSAKTION PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_FB_TXN_BATCH   ON STG_FB_TRANSAKTION (batch_id);
CREATE INDEX IDX_STG_FB_TXN_KEY     ON STG_FB_TRANSAKTION (transaktions_id, fonds_id);
CREATE INDEX IDX_STG_FB_TXN_DATUM   ON STG_FB_TRANSAKTION (handelsdatum);

COMMENT ON TABLE  STG_FB_TRANSAKTION                    IS 'Staging: Wertpapiertransaktionen aus Fondsbuchhaltung (1:1)';
COMMENT ON COLUMN STG_FB_TRANSAKTION.transaktionstyp    IS 'Art der Transaktion: KAUF, VERKAUF, TAUSCH, KAPITALERHOEHUNG, etc.';
COMMENT ON COLUMN STG_FB_TRANSAKTION.stueckzinsen       IS 'Stueckzinsen bei Anleihen';


-- STG_FB_FONDSBEWEGUNG: Mittelzu-/abfluesse (Anteilsscheingeschaefte)
CREATE TABLE STG_FB_FONDSBEWEGUNG (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    bewegungs_id        VARCHAR2(100)   NOT NULL,
    fonds_id            VARCHAR2(50)    NOT NULL,
    -- Bewegungsdetails
    bewegungstyp        VARCHAR2(30)    NOT NULL,  -- AUSGABE, RUECKNAHME, AUSSCHUETTUNG
    handelsdatum        DATE            NOT NULL,
    valutatdatum        DATE,
    buchungsdatum       DATE,
    -- Mengen- und Wertdaten
    anteile             NUMBER(20,6)    NOT NULL,
    anteilswert_nav     NUMBER(20,6),
    ausgabeaufschlag    NUMBER(10,4),
    bruttobetrag        NUMBER(20,2),
    nettobetrag         NUMBER(20,2),
    -- Anleger-/Depotdaten
    depot_nr            VARCHAR2(50),
    depotstelle         VARCHAR2(100),
    anleger_id          VARCHAR2(100),
    -- Waehrung
    waehrung            VARCHAR2(3),
    -- Metadaten Quelle
    src_bewegungs_nr    VARCHAR2(100),
    src_status          VARCHAR2(50),
    src_erstellt_am     DATE,
    src_geaendert_am    DATE,
    CONSTRAINT PK_STG_FB_FONDSBEWEGUNG PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_FB_FBW_BATCH   ON STG_FB_FONDSBEWEGUNG (batch_id);
CREATE INDEX IDX_STG_FB_FBW_KEY     ON STG_FB_FONDSBEWEGUNG (bewegungs_id, fonds_id);
CREATE INDEX IDX_STG_FB_FBW_DATUM   ON STG_FB_FONDSBEWEGUNG (handelsdatum);

COMMENT ON TABLE  STG_FB_FONDSBEWEGUNG                  IS 'Staging: Fondsbewegungen (Ausgaben/Ruecknahmen) aus Fondsbuchhaltung (1:1)';
COMMENT ON COLUMN STG_FB_FONDSBEWEGUNG.bewegungstyp     IS 'Typ der Fondsbewegung: AUSGABE, RUECKNAHME, AUSSCHUETTUNG, THESAURIERUNG';
COMMENT ON COLUMN STG_FB_FONDSBEWEGUNG.anteilswert_nav  IS 'Nettoinventarwert (NAV) pro Anteil zum Handelstag';


-- -----------------------------------------------------------------------------
-- 2. Quellsystem: KURSVERSORGUNG
-- -----------------------------------------------------------------------------

-- STG_KV_KURS: Marktpreise/Kurse aus dem Kursversorgungssystem
CREATE TABLE STG_KV_KURS (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    isin                VARCHAR2(12)    NOT NULL,
    kursdatum           DATE            NOT NULL,
    kurszeit            VARCHAR2(8),    -- HH:MI:SS
    kurstyp             VARCHAR2(30)    NOT NULL,  -- SCHLUSS, GELD, BRIEF, INTRADAY, NAV
    -- Kursdaten
    kurs                NUMBER(20,6)    NOT NULL,
    waehrung            VARCHAR2(3)     NOT NULL,
    -- Volumen-/Umsatzdaten (optional)
    handelsvolumen      NUMBER(20,2),
    umsatz_stueck       NUMBER(20,6),
    -- Boerse/Handelsplatz
    boerse              VARCHAR2(50),
    boerse_kuerzel      VARCHAR2(10),
    -- Qualitaetsindikatoren
    kurs_quelle         VARCHAR2(100),
    kurs_qualitaet      VARCHAR2(20),   -- LIVE, DELAYED, EOD, ESTIMATED
    -- Metadaten Quelle
    src_kurs_id         VARCHAR2(100),
    src_erstellt_am     TIMESTAMP,
    CONSTRAINT PK_STG_KV_KURS PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_KV_KURS_BATCH  ON STG_KV_KURS (batch_id);
CREATE INDEX IDX_STG_KV_KURS_KEY    ON STG_KV_KURS (isin, kursdatum, kurstyp);

COMMENT ON TABLE  STG_KV_KURS                IS 'Staging: Marktpreise/Kurse aus Kursversorgungssystem (1:1)';
COMMENT ON COLUMN STG_KV_KURS.kurstyp        IS 'Art des Kurses: SCHLUSS, GELD, BRIEF, INTRADAY, NAV, REFERENZ';
COMMENT ON COLUMN STG_KV_KURS.kurs_qualitaet IS 'Qualitaet der Kurslieferung: LIVE, DELAYED, EOD, ESTIMATED';


-- STG_KV_WECHSELKURS: Devisenkurse/Wechselkurse
CREATE TABLE STG_KV_WECHSELKURS (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    waehrung_von        VARCHAR2(3)     NOT NULL,   -- Basiswaehrung
    waehrung_nach       VARCHAR2(3)     NOT NULL,   -- Zielwaehrung
    kursdatum           DATE            NOT NULL,
    kurstyp             VARCHAR2(20)    NOT NULL,   -- EZB, REFERENZ, BRIEF, GELD
    -- Kursdaten
    kurs                NUMBER(20,8)    NOT NULL,
    -- Metadaten Quelle
    src_kurs_id         VARCHAR2(100),
    src_erstellt_am     TIMESTAMP,
    CONSTRAINT PK_STG_KV_WECHSELKURS PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_KV_WK_BATCH    ON STG_KV_WECHSELKURS (batch_id);
CREATE INDEX IDX_STG_KV_WK_KEY      ON STG_KV_WECHSELKURS (waehrung_von, waehrung_nach, kursdatum);

COMMENT ON TABLE STG_KV_WECHSELKURS IS 'Staging: Devisenkurse/Wechselkurse aus Kursversorgungssystem (1:1)';


-- -----------------------------------------------------------------------------
-- 3. Quellsystem: WERTPAPIERSTAMMDATEN
-- -----------------------------------------------------------------------------

-- STG_WP_STAMMDATEN: Wertpapier-Stammdaten (securities master data)
CREATE TABLE STG_WP_STAMMDATEN (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Identifikatoren
    isin                VARCHAR2(12)    NOT NULL,
    wkn                 VARCHAR2(6),
    valor               VARCHAR2(20),
    cusip               VARCHAR2(9),
    sedol               VARCHAR2(7),
    ticker              VARCHAR2(20),
    -- Bezeichnungen
    bezeichnung_lang    VARCHAR2(500)   NOT NULL,
    bezeichnung_kurz    VARCHAR2(100),
    -- Klassifikation
    wertpapiertyp       VARCHAR2(50)    NOT NULL,  -- AKTIE, ANLEIHE, FONDS, ETF, DERIVAT, etc.
    asset_klasse        VARCHAR2(50),
    sub_asset_klasse    VARCHAR2(50),
    -- Emittent/Unternehmen
    emittent_name       VARCHAR2(500),
    emittent_isin       VARCHAR2(12),
    land_emittent       VARCHAR2(3),   -- ISO 3166-1 Alpha-3
    branche             VARCHAR2(100),
    branche_code        VARCHAR2(20),  -- GICS oder SIC
    -- Anleihe-spezifische Felder
    nominalwert         NUMBER(20,4),
    nominalwaehrung     VARCHAR2(3),
    fälligkeitsdatum    DATE,
    kuponrate           NUMBER(10,6),
    kuponfrequenz       VARCHAR2(20),
    -- Fonds-spezifische Felder
    fondstyp            VARCHAR2(50),
    ausschuettungsart   VARCHAR2(20),  -- AUSSCHUETTEND, THESAURIEREND
    auflagedatum        DATE,
    -- Waehrung
    handelswährung      VARCHAR2(3),
    notizwaehrung       VARCHAR2(3),
    -- Handelsinformationen
    hauptboerse         VARCHAR2(50),
    boerse_kuerzel      VARCHAR2(10),
    -- Status
    status              VARCHAR2(20),  -- AKTIV, INAKTIV, GELOESCHT
    -- Metadaten Quelle
    src_wertpapier_id   VARCHAR2(100),
    src_erstellt_am     DATE,
    src_geaendert_am    DATE,
    CONSTRAINT PK_STG_WP_STAMMDATEN PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_WP_STM_BATCH   ON STG_WP_STAMMDATEN (batch_id);
CREATE INDEX IDX_STG_WP_STM_ISIN    ON STG_WP_STAMMDATEN (isin);
CREATE INDEX IDX_STG_WP_STM_KEY     ON STG_WP_STAMMDATEN (isin, batch_id);

COMMENT ON TABLE  STG_WP_STAMMDATEN                  IS 'Staging: Wertpapier-Stammdaten (Securities Master Data) (1:1)';
COMMENT ON COLUMN STG_WP_STAMMDATEN.isin             IS 'International Securities Identification Number (12-stellig)';
COMMENT ON COLUMN STG_WP_STAMMDATEN.wkn              IS 'Wertpapierkennnummer (6-stellig, DE)';
COMMENT ON COLUMN STG_WP_STAMMDATEN.wertpapiertyp    IS 'Typ des Wertpapiers: AKTIE, ANLEIHE, FONDS, ETF, DERIVAT, GELDMARKT, ZERTIFIKAT';
COMMENT ON COLUMN STG_WP_STAMMDATEN.ausschuettungsart IS 'Fonds-Ausschuettungsart: AUSSCHUETTEND, THESAURIEREND';


-- STG_WP_FONDS_STAMMDATEN: Fonds-Stammdaten
CREATE TABLE STG_WP_FONDS_STAMMDATEN (
    stg_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    ladezeit            TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Identifikatoren
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12),
    wkn                 VARCHAR2(6),
    -- Bezeichnungen
    fondsname           VARCHAR2(500)   NOT NULL,
    fondsname_kurz      VARCHAR2(100),
    -- Klassifikation
    fondstyp            VARCHAR2(50)    NOT NULL,  -- PUBLIKUMSFONDS, SPEZIALFONDS, ETF, etc.
    investmentstrategie VARCHAR2(200),
    asset_klasse        VARCHAR2(50),
    -- Kapital/NAV
    fondswaehrung       VARCHAR2(3)     NOT NULL,
    auflagedatum        DATE,
    auflegungsvolumen   NUMBER(20,2),
    -- Verwaltungsgesellschaft
    kag_name            VARCHAR2(500),
    kag_bic             VARCHAR2(11),
    verwahrstelle       VARCHAR2(500),
    -- Steuer-/Rechtliches
    domizil_land        VARCHAR2(3),
    rechtliche_struktur VARCHAR2(100),
    ucits_konform       VARCHAR2(1)     DEFAULT 'N' CHECK (ucits_konform IN ('J','N')),
    -- Status
    status              VARCHAR2(20),
    -- Metadaten Quelle
    src_fonds_id        VARCHAR2(100),
    src_erstellt_am     DATE,
    src_geaendert_am    DATE,
    CONSTRAINT PK_STG_WP_FONDS_STM PRIMARY KEY (stg_id)
);

CREATE INDEX IDX_STG_WP_FDS_BATCH   ON STG_WP_FONDS_STAMMDATEN (batch_id);
CREATE INDEX IDX_STG_WP_FDS_KEY     ON STG_WP_FONDS_STAMMDATEN (fonds_id, batch_id);

COMMENT ON TABLE  STG_WP_FONDS_STAMMDATEN              IS 'Staging: Fonds-Stammdaten aus Wertpapierstammdaten-System (1:1)';
COMMENT ON COLUMN STG_WP_FONDS_STAMMDATEN.fondstyp     IS 'Art des Fonds: PUBLIKUMSFONDS, SPEZIALFONDS, ETF, HEDGEFONDS, etc.';
COMMENT ON COLUMN STG_WP_FONDS_STAMMDATEN.ucits_konform IS 'Unterliegt der UCITS-Richtlinie (J/N)';

COMMIT;

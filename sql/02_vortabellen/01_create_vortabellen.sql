-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- Vortabellen (Layer 2: Transformierte und konsolidierte Daten)
-- =============================================================================
-- Beschreibung:
--   Die Vortabellen nehmen die aus den Staging-Tabellen transformierten und
--   konsolidierten Daten auf. Hier werden:
--     - Geschaeftsregeln angewendet
--     - Daten aus verschiedenen Quellen zusammengefuehrt
--     - Datensaetze validiert und bereinigt
--     - SCD (Slowly Changing Dimensions) vorbereitet
--
-- Nomenklatur: VOR_* (Vortabellen-Praefix)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VOR_WERTPAPIER: Konsolidierte Wertpapier-Stammdaten
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_WERTPAPIER (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Identifikatoren
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
    land_iso3           VARCHAR2(3),
    branche             VARCHAR2(100),
    branche_code        VARCHAR2(20),
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
    handelswährung      VARCHAR2(3),
    notizwaehrung       VARCHAR2(3),
    hauptboerse         VARCHAR2(50),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    quellsystem         VARCHAR2(100),
    src_geaendert_am    DATE,
    CONSTRAINT PK_VOR_WERTPAPIER PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_WERTPAPIER_ISIN ON VOR_WERTPAPIER (isin, batch_id);
CREATE INDEX IDX_VOR_WP_BATCH               ON VOR_WERTPAPIER (batch_id);

COMMENT ON TABLE  VOR_WERTPAPIER              IS 'Vortabelle: Konsolidierte und validierte Wertpapier-Stammdaten';
COMMENT ON COLUMN VOR_WERTPAPIER.ist_gueltig  IS 'Datensatz hat Validierung bestanden (J=gueltig, N=fehlerhaft)';


-- -----------------------------------------------------------------------------
-- VOR_FONDS: Konsolidierte Fonds-Stammdaten
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_FONDS (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Identifikatoren
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
    -- Kapital/NAV
    fondswaehrung       VARCHAR2(3)     NOT NULL,
    auflagedatum        DATE,
    -- Verwaltungsgesellschaft
    kag_name            VARCHAR2(500),
    verwahrstelle       VARCHAR2(500),
    -- Rechtliches
    domizil_land        VARCHAR2(3),
    rechtliche_struktur VARCHAR2(100),
    ucits_konform       VARCHAR2(1),
    -- Status
    status              VARCHAR2(20),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    quellsystem         VARCHAR2(100),
    src_geaendert_am    DATE,
    CONSTRAINT PK_VOR_FONDS PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_FONDS_ID         ON VOR_FONDS (fonds_id, batch_id);
CREATE INDEX IDX_VOR_FONDS_BATCH             ON VOR_FONDS (batch_id);

COMMENT ON TABLE VOR_FONDS IS 'Vortabelle: Konsolidierte und validierte Fonds-Stammdaten';


-- -----------------------------------------------------------------------------
-- VOR_KURS: Konsolidierte und bereinigte Kursdaten
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_KURS (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Schluessel
    isin                VARCHAR2(12)    NOT NULL,
    kursdatum           DATE            NOT NULL,
    kurstyp             VARCHAR2(30)    NOT NULL,
    -- Kursdaten
    kurs                NUMBER(20,6)    NOT NULL,
    waehrung            VARCHAR2(3)     NOT NULL,
    -- Ergaenzende Daten
    boerse              VARCHAR2(50),
    kurs_quelle         VARCHAR2(100),
    kurs_qualitaet      VARCHAR2(20),
    -- Normalisierter Kurs in EUR (fuer Vergleichbarkeit)
    kurs_in_eur         NUMBER(20,6),
    wechselkurs_eur     NUMBER(15,8),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    src_kurs_id         VARCHAR2(100),
    CONSTRAINT PK_VOR_KURS PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_KURS_KEY ON VOR_KURS (isin, kursdatum, kurstyp, batch_id);
CREATE INDEX IDX_VOR_KURS_BATCH      ON VOR_KURS (batch_id);
CREATE INDEX IDX_VOR_KURS_ISIN_DT    ON VOR_KURS (isin, kursdatum);

COMMENT ON TABLE  VOR_KURS              IS 'Vortabelle: Bereinigte und normalisierte Kurs- und Preisdaten';
COMMENT ON COLUMN VOR_KURS.kurs_in_eur IS 'Kurs umgerechnet in EUR fuer systemweite Vergleichbarkeit';


-- -----------------------------------------------------------------------------
-- VOR_WECHSELKURS: Konsolidierte Wechselkurse
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_WECHSELKURS (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Schluessel
    waehrung_von        VARCHAR2(3)     NOT NULL,
    waehrung_nach       VARCHAR2(3)     NOT NULL,
    kursdatum           DATE            NOT NULL,
    kurstyp             VARCHAR2(20)    NOT NULL,
    -- Kursdaten
    kurs                NUMBER(20,8)    NOT NULL,
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    CONSTRAINT PK_VOR_WECHSELKURS PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_WK_KEY ON VOR_WECHSELKURS (waehrung_von, waehrung_nach, kursdatum, kurstyp, batch_id);
CREATE INDEX IDX_VOR_WK_BATCH      ON VOR_WECHSELKURS (batch_id);

COMMENT ON TABLE VOR_WECHSELKURS IS 'Vortabelle: Bereinigte und konsolidierte Wechselkurse';


-- -----------------------------------------------------------------------------
-- VOR_BESTAND: Transformierte Fondsbestaende
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_BESTAND (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12)    NOT NULL,
    bewertungsdatum     DATE            NOT NULL,
    -- Mengendaten
    nominale            NUMBER(20,6),
    stueckzahl          NUMBER(20,6),
    -- Wertdaten in Papierwaehrung
    einstandskurs       NUMBER(20,6),
    einstandswert       NUMBER(20,2),
    aktueller_kurs      NUMBER(20,6),
    marktwert           NUMBER(20,2),
    -- Wertdaten in Fondswaehrung (konvertiert)
    marktwert_fondswährung  NUMBER(20,2),
    wechselkurs             NUMBER(15,8),
    -- Wertdaten in EUR (normalisiert)
    marktwert_eur       NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Gewichtung im Portfolio
    portfoliogewicht    NUMBER(10,6),
    -- Waehrung
    waehrung_papier     VARCHAR2(3),
    waehrung_fonds      VARCHAR2(3),
    -- Depot
    depot_nr            VARCHAR2(50),
    depotstelle         VARCHAR2(100),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    stg_id              NUMBER,
    CONSTRAINT PK_VOR_BESTAND PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_BESTAND_KEY ON VOR_BESTAND (fonds_id, isin, bewertungsdatum, batch_id);
CREATE INDEX IDX_VOR_BESTAND_BATCH      ON VOR_BESTAND (batch_id);
CREATE INDEX IDX_VOR_BESTAND_FONDS_DT   ON VOR_BESTAND (fonds_id, bewertungsdatum);

COMMENT ON TABLE  VOR_BESTAND                  IS 'Vortabelle: Transformierte und normalisierte Fondsbestaende';
COMMENT ON COLUMN VOR_BESTAND.portfoliogewicht IS 'Prozentualer Anteil des Wertpapiers am Fondsvermögen';


-- -----------------------------------------------------------------------------
-- VOR_TRANSAKTION: Transformierte Wertpapiertransaktionen
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_TRANSAKTION (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    transaktions_id     VARCHAR2(100)   NOT NULL,
    fonds_id            VARCHAR2(50)    NOT NULL,
    isin                VARCHAR2(12)    NOT NULL,
    -- Transaktionsdetails
    transaktionstyp     VARCHAR2(20)    NOT NULL,
    transaktionstyp_normiert VARCHAR2(20) NOT NULL,  -- normierter Typ: KAUF, VERKAUF, SONSTIGE
    handelsdatum        DATE            NOT NULL,
    valutatdatum        DATE,
    buchungsdatum       DATE,
    -- Mengen- und Wertdaten
    stueckzahl          NUMBER(20,6),
    nominale            NUMBER(20,6),
    kurs                NUMBER(20,6),
    transaktionswert    NUMBER(20,2),
    provision           NUMBER(20,2)    DEFAULT 0,
    stueckzinsen        NUMBER(20,2)    DEFAULT 0,
    gesamtbetrag        NUMBER(20,2),
    -- Wertdaten in EUR (normalisiert)
    gesamtbetrag_eur    NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Waehrung
    waehrung            VARCHAR2(3),
    -- Gegenpartei / Depot
    kontrahent          VARCHAR2(200),
    depot_nr            VARCHAR2(50),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    stg_id              NUMBER,
    CONSTRAINT PK_VOR_TRANSAKTION PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_TXN_KEY     ON VOR_TRANSAKTION (transaktions_id, fonds_id, batch_id);
CREATE INDEX IDX_VOR_TXN_BATCH          ON VOR_TRANSAKTION (batch_id);
CREATE INDEX IDX_VOR_TXN_FONDS_DATUM    ON VOR_TRANSAKTION (fonds_id, handelsdatum);
CREATE INDEX IDX_VOR_TXN_ISIN           ON VOR_TRANSAKTION (isin);

COMMENT ON TABLE  VOR_TRANSAKTION                       IS 'Vortabelle: Transformierte und normalisierte Wertpapiertransaktionen';
COMMENT ON COLUMN VOR_TRANSAKTION.transaktionstyp_normiert IS 'Normierter Transaktionstyp: KAUF, VERKAUF, SONSTIGE (fuer vereinfachte Auswertung)';


-- -----------------------------------------------------------------------------
-- VOR_FONDSBEWEGUNG: Transformierte Fondsbewegungen
-- -----------------------------------------------------------------------------
CREATE TABLE VOR_FONDSBEWEGUNG (
    vor_id              NUMBER          GENERATED ALWAYS AS IDENTITY,
    batch_id            NUMBER          NOT NULL,
    verarbeitungszeit   TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    -- Fachliche Schluessel
    bewegungs_id        VARCHAR2(100)   NOT NULL,
    fonds_id            VARCHAR2(50)    NOT NULL,
    -- Bewegungsdetails
    bewegungstyp        VARCHAR2(30)    NOT NULL,
    bewegungstyp_normiert VARCHAR2(30)  NOT NULL,  -- ZUFLUSS, ABFLUSS, NEUTRAL
    handelsdatum        DATE            NOT NULL,
    valutatdatum        DATE,
    buchungsdatum       DATE,
    -- Mengen- und Wertdaten
    anteile             NUMBER(20,6)    NOT NULL,
    anteilswert_nav     NUMBER(20,6),
    bruttobetrag        NUMBER(20,2),
    nettobetrag         NUMBER(20,2),
    ausgabeaufschlag    NUMBER(20,2)    DEFAULT 0,
    -- Wertdaten in EUR (normalisiert)
    nettobetrag_eur     NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Anleger-/Depotdaten
    depot_nr            VARCHAR2(50),
    anleger_id          VARCHAR2(100),
    -- Waehrung
    waehrung            VARCHAR2(3),
    -- Validierungsstatus
    ist_gueltig         VARCHAR2(1)     DEFAULT 'J' CHECK (ist_gueltig IN ('J','N')),
    validierungsfehler  VARCHAR2(4000),
    -- Quell-Tracking
    stg_id              NUMBER,
    CONSTRAINT PK_VOR_FONDSBEWEGUNG PRIMARY KEY (vor_id)
);

CREATE UNIQUE INDEX UIX_VOR_FBW_KEY      ON VOR_FONDSBEWEGUNG (bewegungs_id, fonds_id, batch_id);
CREATE INDEX IDX_VOR_FBW_BATCH           ON VOR_FONDSBEWEGUNG (batch_id);
CREATE INDEX IDX_VOR_FBW_FONDS_DATUM     ON VOR_FONDSBEWEGUNG (fonds_id, handelsdatum);

COMMENT ON TABLE  VOR_FONDSBEWEGUNG                        IS 'Vortabelle: Transformierte und normalisierte Fondsbewegungen (Ausgaben/Ruecknahmen)';
COMMENT ON COLUMN VOR_FONDSBEWEGUNG.bewegungstyp_normiert  IS 'Normierter Bewegungstyp: ZUFLUSS (Ausgabe), ABFLUSS (Ruecknahme), NEUTRAL (Ausschuettung)';

COMMIT;

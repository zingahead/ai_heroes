-- =============================================================================
-- Datawarehouse Asset Management - Oracle 19c
-- Faktentabellen (Layer 3: Kennzahlen und Bewegungsdaten)
-- =============================================================================
-- Beschreibung:
--   Faktentabellen enthalten die messbaren Geschaeftsvorfaelle und Kennzahlen
--   des Asset Managements. Sie referenzieren die Dimensionstabellen ueber
--   Surrogat-Schluessel (SK).
--
-- Faktentabellen:
--   FAKT_BESTAND          - Tagesaktuelle Fondsbestaende (Snapshot)
--   FAKT_TRANSAKTION      - Wertpapiertransaktionen (Kaeufe/Verkaeufe)
--   FAKT_FONDSBEWEGUNG    - Mittelzu-/abfluesse (Anteilsscheingeschaefte)
--   FAKT_KURS             - Marktpreise und Kurse je Wertpapier und Tag
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FAKT_BESTAND: Tagesaktuelle Fondsbestaende (Periodenmomentaufnahme)
-- -----------------------------------------------------------------------------
CREATE TABLE FAKT_BESTAND (
    bestand_id          NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- Zeitdimension
    zeit_id             NUMBER          NOT NULL
                            REFERENCES DIM_ZEIT(zeit_id),       -- Bewertungsdatum
    -- Dimensionsreferenzen (Surrogatschluessel)
    fonds_sk            NUMBER          NOT NULL
                            REFERENCES DIM_FONDS(fonds_sk),
    wertpapier_sk       NUMBER          NOT NULL
                            REFERENCES DIM_WERTPAPIER(wertpapier_sk),
    depot_sk            NUMBER
                            REFERENCES DIM_DEPOT(depot_sk),
    waehrung_papier_id  NUMBER
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    waehrung_fonds_id   NUMBER
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Mengenkennzahlen
    nominale            NUMBER(20,6),
    stueckzahl          NUMBER(20,6),
    -- Wertkennzahlen in Papierwaehrung
    einstandskurs       NUMBER(20,6),
    einstandswert       NUMBER(20,2),
    aktueller_kurs      NUMBER(20,6),
    marktwert           NUMBER(20,2),
    unrealisierter_gv   NUMBER(20,2),   -- Unrealisierter Gewinn/Verlust
    -- Wertkennzahlen in Fondswaehrung
    marktwert_fondswährung  NUMBER(20,2),
    einstandswert_fondswährung NUMBER(20,2),
    wechselkurs             NUMBER(15,8),
    -- Wertkennzahlen in EUR (Reporting-Waehrung)
    marktwert_eur       NUMBER(20,2),
    einstandswert_eur   NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Portfolio-Kennzahlen
    portfoliogewicht    NUMBER(10,6),   -- Anteil am Fondsvermögen in %
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id            NUMBER          NOT NULL,
    -- Quell-Tracking
    fonds_id            VARCHAR2(50),   -- Fachlicher Schluessel (Redundanz fuer Debug)
    isin                VARCHAR2(12),   -- Fachlicher Schluessel (Redundanz fuer Debug)
    CONSTRAINT PK_FAKT_BESTAND PRIMARY KEY (bestand_id)
);

-- Partitionierung nach Bewertungsdatum (Monat) - typisch fuer grosse Bestandstabellen
-- Hinweis: Fuer produktiven Einsatz sollte hier LIST oder RANGE-Partitionierung aktiviert werden
CREATE INDEX IDX_FAKT_BST_ZEIT          ON FAKT_BESTAND (zeit_id);
CREATE INDEX IDX_FAKT_BST_FONDS         ON FAKT_BESTAND (fonds_sk, zeit_id);
CREATE INDEX IDX_FAKT_BST_WP            ON FAKT_BESTAND (wertpapier_sk, zeit_id);
CREATE INDEX IDX_FAKT_BST_BATCH         ON FAKT_BESTAND (batch_id);
CREATE UNIQUE INDEX UIX_FAKT_BST_KEY    ON FAKT_BESTAND (fonds_sk, wertpapier_sk, zeit_id, NVL(depot_sk,0));

COMMENT ON TABLE  FAKT_BESTAND                    IS 'Faktentabelle: Tagesaktuelle Fondsbestaende (Snapshot-Faktentabelle)';
COMMENT ON COLUMN FAKT_BESTAND.bestand_id         IS 'Technischer Surrogatschluessel';
COMMENT ON COLUMN FAKT_BESTAND.zeit_id            IS 'FK zur Zeitdimension (Format YYYYMMDD = Bewertungsdatum)';
COMMENT ON COLUMN FAKT_BESTAND.unrealisierter_gv  IS 'Unrealisierter Gewinn (+) oder Verlust (-) in Papierwaehrung';
COMMENT ON COLUMN FAKT_BESTAND.portfoliogewicht   IS 'Prozentualer Anteil des Wertpapiers am gesamten Fondsvermögen';


-- -----------------------------------------------------------------------------
-- FAKT_TRANSAKTION: Wertpapiertransaktionen (Transaktions-Faktentabelle)
-- -----------------------------------------------------------------------------
CREATE TABLE FAKT_TRANSAKTION (
    transaktion_id      NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- Zeitdimensionen
    zeit_id_handel      NUMBER          NOT NULL
                            REFERENCES DIM_ZEIT(zeit_id),       -- Handelsdatum
    zeit_id_valuta      NUMBER
                            REFERENCES DIM_ZEIT(zeit_id),       -- Valutatdatum
    zeit_id_buchung     NUMBER
                            REFERENCES DIM_ZEIT(zeit_id),       -- Buchungsdatum
    -- Dimensionsreferenzen
    fonds_sk            NUMBER          NOT NULL
                            REFERENCES DIM_FONDS(fonds_sk),
    wertpapier_sk       NUMBER          NOT NULL
                            REFERENCES DIM_WERTPAPIER(wertpapier_sk),
    depot_sk            NUMBER
                            REFERENCES DIM_DEPOT(depot_sk),
    waehrung_id         NUMBER
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Fachliche Schluessel (als Referenz)
    transaktions_id     VARCHAR2(100)   NOT NULL,
    -- Transaktionsattribute
    transaktionstyp     VARCHAR2(20)    NOT NULL,               -- KAUF, VERKAUF, TAUSCH, etc.
    transaktionstyp_normiert VARCHAR2(20) NOT NULL,             -- KAUF, VERKAUF, SONSTIGE
    -- Mengenkennzahlen
    stueckzahl          NUMBER(20,6),
    nominale            NUMBER(20,6),
    -- Wertkennzahlen in Transaktionswaehrung
    kurs                NUMBER(20,6),
    transaktionswert    NUMBER(20,2),
    provision           NUMBER(20,2)    DEFAULT 0,
    stueckzinsen        NUMBER(20,2)    DEFAULT 0,
    gesamtbetrag        NUMBER(20,2),
    -- Vorzeichen: +1 fuer Kauf (Abfluss Liquiditaet), -1 fuer Verkauf (Zufluss)
    vorzeichen          NUMBER(2),
    -- Wertkennzahlen in EUR (normalisiert)
    gesamtbetrag_eur    NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Gegenpartei
    kontrahent          VARCHAR2(200),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id            NUMBER          NOT NULL,
    -- Quell-Tracking
    fonds_id            VARCHAR2(50),
    isin                VARCHAR2(12),
    CONSTRAINT PK_FAKT_TRANSAKTION PRIMARY KEY (transaktion_id)
);

CREATE INDEX IDX_FAKT_TXN_HANDEL       ON FAKT_TRANSAKTION (zeit_id_handel);
CREATE INDEX IDX_FAKT_TXN_FONDS        ON FAKT_TRANSAKTION (fonds_sk, zeit_id_handel);
CREATE INDEX IDX_FAKT_TXN_WP           ON FAKT_TRANSAKTION (wertpapier_sk, zeit_id_handel);
CREATE INDEX IDX_FAKT_TXN_BATCH        ON FAKT_TRANSAKTION (batch_id);
CREATE INDEX IDX_FAKT_TXN_ID           ON FAKT_TRANSAKTION (transaktions_id);

COMMENT ON TABLE  FAKT_TRANSAKTION                     IS 'Faktentabelle: Wertpapiertransaktionen (Kaeufe, Verkaeufe, Tauschgeschaefte)';
COMMENT ON COLUMN FAKT_TRANSAKTION.vorzeichen          IS '+1 = Wertpapier-Zugang (Kauf), -1 = Wertpapier-Abgang (Verkauf)';
COMMENT ON COLUMN FAKT_TRANSAKTION.transaktions_id     IS 'Fachlicher Transaktionsschluessel aus dem Quellsystem';


-- -----------------------------------------------------------------------------
-- FAKT_FONDSBEWEGUNG: Mittelzu-/abfluesse (Transaktions-Faktentabelle)
-- -----------------------------------------------------------------------------
CREATE TABLE FAKT_FONDSBEWEGUNG (
    fondsbewegung_id    NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- Zeitdimensionen
    zeit_id_handel      NUMBER          NOT NULL
                            REFERENCES DIM_ZEIT(zeit_id),
    zeit_id_valuta      NUMBER
                            REFERENCES DIM_ZEIT(zeit_id),
    zeit_id_buchung     NUMBER
                            REFERENCES DIM_ZEIT(zeit_id),
    -- Dimensionsreferenzen
    fonds_sk            NUMBER          NOT NULL
                            REFERENCES DIM_FONDS(fonds_sk),
    depot_sk            NUMBER
                            REFERENCES DIM_DEPOT(depot_sk),
    waehrung_id         NUMBER
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Fachlicher Schluessel
    bewegungs_id        VARCHAR2(100)   NOT NULL,
    -- Bewegungsattribute
    bewegungstyp        VARCHAR2(30)    NOT NULL,               -- AUSGABE, RUECKNAHME, AUSSCHUETTUNG
    bewegungstyp_normiert VARCHAR2(30)  NOT NULL,               -- ZUFLUSS, ABFLUSS, NEUTRAL
    -- Mengenkennzahlen
    anteile             NUMBER(20,6)    NOT NULL,
    anteilswert_nav     NUMBER(20,6),
    -- Wertkennzahlen
    ausgabeaufschlag    NUMBER(20,2)    DEFAULT 0,
    bruttobetrag        NUMBER(20,2),
    nettobetrag         NUMBER(20,2),
    -- Vorzeichen: +1 fuer Ausgabe (Zufluss), -1 fuer Ruecknahme (Abfluss)
    vorzeichen          NUMBER(2),
    -- Wertkennzahlen in EUR (normalisiert)
    nettobetrag_eur     NUMBER(20,2),
    wechselkurs_eur     NUMBER(15,8),
    -- Anleger
    anleger_id          VARCHAR2(100),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id            NUMBER          NOT NULL,
    -- Quell-Tracking
    fonds_id            VARCHAR2(50),
    CONSTRAINT PK_FAKT_FONDSBEWEGUNG PRIMARY KEY (fondsbewegung_id)
);

CREATE INDEX IDX_FAKT_FBW_HANDEL       ON FAKT_FONDSBEWEGUNG (zeit_id_handel);
CREATE INDEX IDX_FAKT_FBW_FONDS        ON FAKT_FONDSBEWEGUNG (fonds_sk, zeit_id_handel);
CREATE INDEX IDX_FAKT_FBW_BATCH        ON FAKT_FONDSBEWEGUNG (batch_id);
CREATE INDEX IDX_FAKT_FBW_ID           ON FAKT_FONDSBEWEGUNG (bewegungs_id);

COMMENT ON TABLE  FAKT_FONDSBEWEGUNG               IS 'Faktentabelle: Fondsbewegungen (Ausgaben, Ruecknahmen, Ausschuettungen)';
COMMENT ON COLUMN FAKT_FONDSBEWEGUNG.vorzeichen    IS '+1 = Mittelzufluss (Ausgabe), -1 = Mittelabfluss (Ruecknahme)';
COMMENT ON COLUMN FAKT_FONDSBEWEGUNG.anteile       IS 'Anzahl Fondsanteile (positiv=Ausgabe, negativ=Ruecknahme)';


-- -----------------------------------------------------------------------------
-- FAKT_KURS: Marktpreise und Kurse (Periodenmomentaufnahme)
-- -----------------------------------------------------------------------------
CREATE TABLE FAKT_KURS (
    kurs_id             NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- Zeitdimension
    zeit_id             NUMBER          NOT NULL
                            REFERENCES DIM_ZEIT(zeit_id),       -- Kursdatum
    -- Dimensionsreferenzen
    wertpapier_sk       NUMBER          NOT NULL
                            REFERENCES DIM_WERTPAPIER(wertpapier_sk),
    waehrung_id         NUMBER          NOT NULL
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Kurstyp
    kurstyp             VARCHAR2(30)    NOT NULL,               -- SCHLUSS, GELD, BRIEF, NAV
    -- Kurskennzahlen
    kurs                NUMBER(20,6)    NOT NULL,
    -- Ergaenzende Kurskennzahlen (optional, je nach Verfuegbarkeit)
    kurs_hoch           NUMBER(20,6),
    kurs_tief           NUMBER(20,6),
    kurs_eroeffnung     NUMBER(20,6),
    handelsvolumen      NUMBER(20,2),
    umsatz_stueck       NUMBER(20,6),
    -- Kurs in EUR (Reporting-Waehrung, normalisiert)
    kurs_in_eur         NUMBER(20,6),
    wechselkurs_eur     NUMBER(15,8),
    -- Kursqualitaet
    boerse              VARCHAR2(50),
    kurs_quelle         VARCHAR2(100),
    kurs_qualitaet      VARCHAR2(20),
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id            NUMBER          NOT NULL,
    -- Quell-Tracking
    isin                VARCHAR2(12),
    CONSTRAINT PK_FAKT_KURS PRIMARY KEY (kurs_id)
);

CREATE UNIQUE INDEX UIX_FAKT_KURS_KEY   ON FAKT_KURS (wertpapier_sk, zeit_id, kurstyp, NVL(boerse,'XDEFAULT'));
CREATE INDEX IDX_FAKT_KURS_ZEIT         ON FAKT_KURS (zeit_id);
CREATE INDEX IDX_FAKT_KURS_WP           ON FAKT_KURS (wertpapier_sk, zeit_id);
CREATE INDEX IDX_FAKT_KURS_BATCH        ON FAKT_KURS (batch_id);
CREATE INDEX IDX_FAKT_KURS_ISIN         ON FAKT_KURS (isin, zeit_id);

COMMENT ON TABLE  FAKT_KURS                IS 'Faktentabelle: Marktpreise und Kurse je Wertpapier und Handelstag';
COMMENT ON COLUMN FAKT_KURS.kurstyp        IS 'Art des Kurses: SCHLUSS (End-of-Day), GELD (Bid), BRIEF (Ask), NAV (Fondswert), INTRADAY';
COMMENT ON COLUMN FAKT_KURS.kurs_in_eur    IS 'Kurs umgerechnet in EUR fuer systemweites Reporting';


-- -----------------------------------------------------------------------------
-- FAKT_WECHSELKURS: Devisenkurse (als Faktentabelle fuer Reporting)
-- -----------------------------------------------------------------------------
CREATE TABLE FAKT_WECHSELKURS (
    wechselkurs_id      NUMBER          GENERATED ALWAYS AS IDENTITY,
    -- Zeitdimension
    zeit_id             NUMBER          NOT NULL
                            REFERENCES DIM_ZEIT(zeit_id),
    -- Dimensionsreferenzen
    waehrung_von_id     NUMBER          NOT NULL
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    waehrung_nach_id    NUMBER          NOT NULL
                            REFERENCES DIM_WAEHRUNG(waehrung_id),
    -- Kurstyp und Wert
    kurstyp             VARCHAR2(20)    NOT NULL,   -- EZB, REFERENZ, BRIEF, GELD
    kurs                NUMBER(20,8)    NOT NULL,
    -- DWH Metadaten
    erstellt_am         TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    batch_id            NUMBER          NOT NULL,
    CONSTRAINT PK_FAKT_WECHSELKURS PRIMARY KEY (wechselkurs_id)
);

CREATE UNIQUE INDEX UIX_FAKT_WK_KEY ON FAKT_WECHSELKURS (waehrung_von_id, waehrung_nach_id, zeit_id, kurstyp);
CREATE INDEX IDX_FAKT_WK_ZEIT       ON FAKT_WECHSELKURS (zeit_id);
CREATE INDEX IDX_FAKT_WK_BATCH      ON FAKT_WECHSELKURS (batch_id);

COMMENT ON TABLE FAKT_WECHSELKURS IS 'Faktentabelle: Devisenkurse und Wechselkurse je Tag und Waehrungspaar';

COMMIT;

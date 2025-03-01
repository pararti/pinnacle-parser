-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- TABLE: belongs_to
-- ------------------------------

DEFINE TABLE belongs_to TYPE RELATION IN league OUT sport SCHEMALESS PERMISSIONS NONE;



-- ------------------------------
-- TABLE DATA: belongs_to
-- ------------------------------


-- ------------------------------
-- TABLE: bet
-- ------------------------------

DEFINE TABLE bet TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD key ON bet TYPE string PERMISSIONS FULL;
DEFINE FIELD period ON bet TYPE int PERMISSIONS FULL;
DEFINE FIELD prices ON bet VALUE ->has_price->price PERMISSIONS FULL;
DEFINE FIELD side ON bet TYPE string PERMISSIONS FULL;
DEFINE FIELD status ON bet TYPE string PERMISSIONS FULL;
DEFINE FIELD status_flag ON bet TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD timestamp ON bet TYPE datetime DEFAULT time::now() PERMISSIONS FULL;
DEFINE FIELD type ON bet TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: bet
-- ------------------------------


-- ------------------------------
-- TABLE: contains
-- ------------------------------

DEFINE TABLE contains TYPE RELATION IN league OUT match SCHEMALESS PERMISSIONS NONE;



-- ------------------------------
-- TABLE DATA: contains
-- ------------------------------


-- ------------------------------
-- TABLE: event_log
-- ------------------------------

DEFINE TABLE event_log TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD data ON event_log TYPE object PERMISSIONS FULL;
DEFINE FIELD entity_id ON event_log TYPE string PERMISSIONS FULL;
DEFINE FIELD entity_type ON event_log TYPE string PERMISSIONS FULL;
DEFINE FIELD event_type ON event_log TYPE int PERMISSIONS FULL;
DEFINE FIELD processed_at ON event_log TYPE datetime DEFAULT time::now() PERMISSIONS FULL;
DEFINE FIELD source ON event_log TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: event_log
-- ------------------------------


-- ------------------------------
-- TABLE: has_price
-- ------------------------------

DEFINE TABLE has_price TYPE RELATION IN bet OUT price SCHEMALESS PERMISSIONS NONE;



-- ------------------------------
-- TABLE DATA: has_price
-- ------------------------------


-- ------------------------------
-- TABLE: league
-- ------------------------------

DEFINE TABLE league TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD group ON league TYPE string PERMISSIONS FULL;
DEFINE FIELD id ON league TYPE int PERMISSIONS FULL;
DEFINE FIELD is_hidden ON league TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD is_promoted ON league TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD is_sticky ON league TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD name ON league TYPE string PERMISSIONS FULL;
DEFINE FIELD sequence ON league TYPE int PERMISSIONS FULL;
DEFINE FIELD sport ON league VALUE <-belongs_to<-sport PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: league
-- ------------------------------


-- ------------------------------
-- TABLE: match
-- ------------------------------

DEFINE TABLE match TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD best_of_x ON match TYPE int PERMISSIONS FULL;
DEFINE FIELD bets ON match VALUE ->offers->bet PERMISSIONS FULL;
DEFINE FIELD id ON match TYPE int PERMISSIONS FULL;
DEFINE FIELD is_live ON match TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD league ON match VALUE <-contains<-league PERMISSIONS FULL;
DEFINE FIELD parent_id ON match TYPE int PERMISSIONS FULL;
DEFINE FIELD participants ON match VALUE <-participates_in<-participant PERMISSIONS FULL;
DEFINE FIELD start_time ON match TYPE datetime PERMISSIONS FULL;
DEFINE FIELD status_flag ON match TYPE int DEFAULT 0 PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: match
-- ------------------------------


-- ------------------------------
-- TABLE: offers
-- ------------------------------

DEFINE TABLE offers TYPE RELATION IN match OUT bet SCHEMALESS PERMISSIONS NONE;



-- ------------------------------
-- TABLE DATA: offers
-- ------------------------------


-- ------------------------------
-- TABLE: participant
-- ------------------------------

DEFINE TABLE participant TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD alignment ON participant TYPE string PERMISSIONS FULL;
DEFINE FIELD id ON participant TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON participant TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: participant
-- ------------------------------


-- ------------------------------
-- TABLE: participates_in
-- ------------------------------

DEFINE TABLE participates_in TYPE RELATION IN participant OUT match SCHEMALESS PERMISSIONS NONE;



-- ------------------------------
-- TABLE DATA: participates_in
-- ------------------------------


-- ------------------------------
-- TABLE: price
-- ------------------------------

DEFINE TABLE price TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD designation ON price TYPE string PERMISSIONS FULL;
DEFINE FIELD participant_id ON price TYPE int PERMISSIONS FULL;
DEFINE FIELD points ON price TYPE float PERMISSIONS FULL;
DEFINE FIELD price_value ON price TYPE int PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: price
-- ------------------------------


-- ------------------------------
-- TABLE: sport
-- ------------------------------

DEFINE TABLE sport TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD id ON sport TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON sport TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: sport
-- ------------------------------



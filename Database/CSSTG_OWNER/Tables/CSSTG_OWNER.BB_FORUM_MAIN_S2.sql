DROP TABLE CSSTG_OWNER.BB_FORUM_MAIN_S2 CASCADE CONSTRAINTS
/

--
-- BB_FORUM_MAIN_S2  (Table) 
--
CREATE TABLE CSSTG_OWNER.BB_FORUM_MAIN_S2
(
  BB_SOURCE         VARCHAR2(10 CHAR)           NOT NULL,
  PK1               INTEGER                     NOT NULL,
  DTCREATED         DATE,
  DTMODIFIED        DATE,
  CONFMAIN_PK1      INTEGER,
  TEXT_FORMAT_TYPE  CHAR(1 CHAR),
  ORDER_NUM         NUMBER,
  NAME              NVARCHAR2(333),
  AVAILABLE_IND     CHAR(1 CHAR),
  POST_FIRST        CHAR(1 CHAR),
  START_DATE        DATE,
  END_DATE          DATE,
  UUID              NVARCHAR2(32),
  DELETE_FLAG       VARCHAR2(1 CHAR)            DEFAULT 'N',
  INSERT_TIME       DATE                        DEFAULT SYSDATE,
  UPDATE_TIME       DATE                        DEFAULT SYSDATE
)
COMPRESS BASIC
PARTITION BY LIST (BB_SOURCE)
(  
  PARTITION UMAMH VALUES ('UMAMH')
    COMPRESS BASIC,  
  PARTITION UMBOS VALUES ('UMBOS')
    COMPRESS BASIC,  
  PARTITION UMDAR VALUES ('UMDAR')
    COMPRESS BASIC,  
  PARTITION UMLOW VALUES ('UMLOW')
    COMPRESS BASIC,  
  PARTITION UMLOW_DAY VALUES ('UMLOW_DAY')
    COMPRESS BASIC,  
  PARTITION UMOL VALUES ('UMOL')
    COMPRESS BASIC,  
  PARTITION UMWOR VALUES ('UMWOR')
    COMPRESS BASIC
)
/

COMMENT ON TABLE CSSTG_OWNER.BB_FORUM_MAIN_S2 IS 'This table contains general information for each discussion forum.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.DTCREATED IS 'This is the date time when the discussion forum was created.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.DTMODIFIED IS 'This is the date time when the general information for the discurssion forum was last modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.CONFMAIN_PK1 IS 'This is the Foreign Key referencing conference_main table. It is where the discussion forum is created in.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.TEXT_FORMAT_TYPE IS 'This is the text format for the discussion forum body. S = Smart Text, P =  Plain Text, H = HTML.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.ORDER_NUM IS 'This is the display order on the UI.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.NAME IS 'This is the name of the discussion forum.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.AVAILABLE_IND IS 'This is a boolean flag (Y/N) indicates if the discussion forum is available.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.POST_FIRST IS 'This column indicates if the forum is forced to be post first (F) or the threads are able to be reply first (T) or regular (N).'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.START_DATE IS 'This is the date time when the discussion forum would start to be available.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.END_DATE IS 'This is the date time when the discussion forum would end to be available.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.UUID IS 'A unique (generated) identifier for this forum.  Sent as the forum_id in LTI launch requests.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_FORUM_MAIN_S2.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

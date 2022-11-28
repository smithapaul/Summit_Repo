DROP TABLE CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2 CASCADE CONSTRAINTS
/

--
-- UMOL_CLASSIFICATIONS_S2  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2
(
  BB_SOURCE     VARCHAR2(10 CHAR)               NOT NULL,
  PK1           INTEGER                         NOT NULL,
  TITLE         NVARCHAR2(255),
  BATCH_UID     NVARCHAR2(450),
  ROW_STATUS    NUMBER(1),
  DATA_SRC_PK1  INTEGER,
  PARENT_PK1    INTEGER,
  DELETE_FLAG   VARCHAR2(1 CHAR)                DEFAULT 'N',
  INSERT_TIME   DATE                            DEFAULT SYSDATE,
  UPDATE_TIME   DATE                            DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2 IS 'This table contains the classification of courses with builtin self referece classification hierarchy.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.TITLE IS 'This is the title of the classification, it is the subject area of courses.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.BATCH_UID IS 'This field uniquely identified this classification with a format of thistitle:parenttitle where the root is ROOT:ROOT. Do not rely on this relationship for display though - instead use the parent_pk1 relationship'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.ROW_STATUS IS 'Row status: 0=enabled, 1=undefined, 2=disabled.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.DATA_SRC_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].data_source table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.PARENT_PK1 IS 'This is a self reference key to define the hierachy of the classifications.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

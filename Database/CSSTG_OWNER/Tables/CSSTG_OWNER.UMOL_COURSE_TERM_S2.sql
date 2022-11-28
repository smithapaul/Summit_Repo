DROP TABLE CSSTG_OWNER.UMOL_COURSE_TERM_S2 CASCADE CONSTRAINTS
/

--
-- UMOL_COURSE_TERM_S2  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_COURSE_TERM_S2
(
  BB_SOURCE    VARCHAR2(10 CHAR)                NOT NULL,
  PK1          INTEGER                          NOT NULL,
  TERM_PK1     INTEGER,
  CRSMAIN_PK1  INTEGER,
  DELETE_FLAG  VARCHAR2(1 CHAR)                 DEFAULT 'N',
  INSERT_TIME  DATE                             DEFAULT SYSDATE,
  UPDATE_TIME  DATE                             DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_COURSE_TERM_S2 IS 'This table maps courses to terms.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.PK1 IS 'Unique identity'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.TERM_PK1 IS 'FK to term'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.CRSMAIN_PK1 IS 'FK to course_main'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_TERM_S2.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

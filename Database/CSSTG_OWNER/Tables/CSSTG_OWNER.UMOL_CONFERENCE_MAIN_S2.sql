DROP TABLE CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2 CASCADE CONSTRAINTS
/

--
-- UMOL_CONFERENCE_MAIN_S2  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2
(
  BB_SOURCE         VARCHAR2(10 CHAR)           NOT NULL,
  PK1               INTEGER                     NOT NULL,
  DTCREATED         DATE,
  DTMODIFIED        DATE,
  CRSMAIN_PK1       INTEGER,
  GROUPS_PK1        INTEGER,
  AVAILABLE_IND     CHAR(1 CHAR),
  TEXT_FORMAT_TYPE  CHAR(1 CHAR),
  ORDER_NUM         NUMBER,
  NAME              NVARCHAR2(255),
  ICON              VARCHAR2(255 CHAR),
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2 IS 'This table contains general information for discussion boards. A conference is a discussion board that acts as a container for various discussion forums.
       Each course/organization/group has a default conference that are automatically created. Community Engagement licensees can also create conferences that can be accessed by all users on the system.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.DTCREATED IS 'This is the date time when the discussion board record was created.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.DTMODIFIED IS 'This is the date time when the discussion board record was last modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.CRSMAIN_PK1 IS 'This is the foreign key referencing course_main table. If it is null than indicates  SYSTEM / institution/community discussions. If it is not null and groups_pk1 is null it is the COURSE discussion. If it is not null and groups_pk1 is not null it is GROUPS discussion.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.GROUPS_PK1 IS 'This is the foreign key referencing groups table. If it is null and the crsmain_pk1 is not null then it is COURSE not GROUP discussion.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.AVAILABLE_IND IS 'This is a boolean flag (Y/N) that indicates if the dicussion board is available to users.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.TEXT_FORMAT_TYPE IS 'This is the text format defined for the discussion board. S = Smart Text, P = Plain Text, H = HTML.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.ORDER_NUM IS 'This is the display order of the discussion board.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.NAME IS 'This is the name of the discussion board.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.ICON IS 'This is the file name of the icon that was select to be associated to the discussion board. There are a fixed number of provided images to choose from - these only apply to the Community Discussion Boards.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

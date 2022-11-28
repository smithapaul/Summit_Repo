DROP TABLE CSSTG_OWNER.BB_GROUPS_S1 CASCADE CONSTRAINTS
/

--
-- BB_GROUPS_S1  (Table) 
--
CREATE TABLE CSSTG_OWNER.BB_GROUPS_S1
(
  BB_SOURCE                     VARCHAR2(10 CHAR) NOT NULL,
  PK1                           INTEGER         NOT NULL,
  CRSMAIN_PK1                   INTEGER         NOT NULL,
  DTMODIFIED                    DATE,
  AVAILABLE_IND                 CHAR(1 CHAR),
  STUDENT_CREATE_FORUM_IND      CHAR(1 CHAR),
  SET_PK1                       INTEGER,
  SET_IND                       CHAR(1 CHAR),
  SELF_ENROLL_IND               CHAR(1 CHAR),
  SHOW_SELF_ENROLL_IND          CHAR(1 CHAR),
  CUSTOMIZE_IND                 CHAR(1 CHAR),
  SIGNUP_IND                    CHAR(1 CHAR),
  ENROLL_LIMIT                  NUMBER,
  TEXT_FORMAT_TYPE              CHAR(1 CHAR),
  GROUP_NAME                    NVARCHAR2(333),
  TAB_PK1                       INTEGER,
  ALLOW_EDIT_TO_GROUP_IND       CHAR(1 CHAR),
  BATCH_UID                     NVARCHAR2(256),
  CREATE_COLLAB_RECORDINGS_IND  CHAR(1 CHAR),
  UUID                          NVARCHAR2(32),
  DELETE_FLAG                   VARCHAR2(1 CHAR) DEFAULT 'N',
  INSERT_TIME                   DATE            DEFAULT SYSDATE,
  UPDATE_TIME                   DATE            DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.BB_GROUPS_S1 IS 'This table contains general information on a group.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.CRSMAIN_PK1 IS 'This is the Foreign Key referencing course_main table. This is the course in which the group or group set was created.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.DTMODIFIED IS 'This is the date time when the group or group set record was last modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.AVAILABLE_IND IS 'This is a boolean flag (Y/N) that indicates if the group or grop set is available.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.STUDENT_CREATE_FORUM_IND IS 'This is a boolean flag (Y/N) that indicates if the students in a group can create a forum'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.SET_PK1 IS 'This is the Foreign Key self referencing groups table. This is the group set PK if this group is part of a group set.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.SET_IND IS 'This is a boolean flag (Y/N) that indicates if it is a group set vs. a group.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.SELF_ENROLL_IND IS 'This is a boolean flag (Y/N) that indicates if the group or group set is a self enroll group or group set.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.SHOW_SELF_ENROLL_IND IS 'This is a boolean flag (Y/N) that indicates sign up option displayed or not.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.CUSTOMIZE_IND IS 'This is a boolean flag (Y/N) that indicates if the group allows for module personalization.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.SIGNUP_IND IS 'This is a boolean flag (Y/N) that indicates if the group availability is set to sign-up sheet only.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.ENROLL_LIMIT IS 'This is maximum number of members allow to enroll in this group.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.TEXT_FORMAT_TYPE IS 'This is the text format of the group or group set. P = Plain text, H = HTML, S = Smart text, X = Default'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.GROUP_NAME IS 'This is the name of the group.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.TAB_PK1 IS 'This is the Foreign Key self referencing tab table. This is the group set PK if this group is part of a group set.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.ALLOW_EDIT_TO_GROUP_IND IS 'This is a boolean flag (Y/N) that indicates group edit rights.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.BATCH_UID IS 'The unique id for the group. Used to store external identifiers for the group.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.CREATE_COLLAB_RECORDINGS_IND IS 'This is a boolean flag (Y/N) that indicates if the students in a group collaborate session can create a recording'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.UUID IS 'Universally unique id used to reference the group in external systems.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_GROUPS_S1.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

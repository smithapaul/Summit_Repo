DROP TABLE CSSTG_OWNER.BB_INSTITUTION_ROLES_S1 CASCADE CONSTRAINTS
/

--
-- BB_INSTITUTION_ROLES_S1  (Table) 
--
CREATE TABLE CSSTG_OWNER.BB_INSTITUTION_ROLES_S1
(
  BB_SOURCE            VARCHAR2(10 CHAR)        NOT NULL,
  PK1                  INTEGER                  NOT NULL,
  ROLE_NAME            NVARCHAR2(50),
  DESCRIPTION          NVARCHAR2(1000),
  ROLE_ID              NVARCHAR2(50),
  DATA_SRC_PK1         INTEGER,
  GUEST_IND            CHAR(1 CHAR),
  REMOVABLE_IND        CHAR(1 CHAR),
  SELF_SELECTABLE_IND  CHAR(1 CHAR),
  ROW_STATUS           INTEGER,
  DELETE_FLAG          VARCHAR2(1 CHAR)         DEFAULT 'N',
  INSERT_TIME          DATE                     DEFAULT SYSDATE,
  UPDATE_TIME          DATE                     DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.BB_INSTITUTION_ROLES_S1 IS 'This table contains information on institution roles. Institution Roles control access to information and services. Access to tabs, modules, and brands can be controlled by assigning users different Institution Roles. Institution roles also grant or deny access to Content Collection files and folders.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.ROLE_NAME IS 'This the name of the institution role as it appears to users. This is the name that shows on the UI.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.DESCRIPTION IS 'This is the description of the institution role.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.ROLE_ID IS 'This is the code for the institution role. The unique identifier of the role. This is the key that is used to identify the role during Snapshot and other data management operations. This id uniquely identifies the record.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.DATA_SRC_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].data_source table.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.GUEST_IND IS 'This is the flag that indicates the role is a guest role. This indicates whether or not the role serves as a Guest role for a particular brand.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.REMOVABLE_IND IS 'This is the flag that indicates if the institution role is removable. This indicates whether or not the role may be removed from the system. Some institution roles are not removable to ensure backward compatibility.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.SELF_SELECTABLE_IND IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.ROW_STATUS IS 'Row status: 0=enabled, 1=undefined/deleted, 2=disabled.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_INSTITUTION_ROLES_S1.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

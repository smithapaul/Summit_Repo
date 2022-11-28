DROP TABLE CSSTG_OWNER.UMOL_USERS_S1 CASCADE CONSTRAINTS
/

--
-- UMOL_USERS_S1  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_USERS_S1
(
  BB_SOURCE              VARCHAR2(10 CHAR)      NOT NULL,
  PK1                    INTEGER                NOT NULL,
  CITY                   NVARCHAR2(50),
  DATA_SRC_PK1           INTEGER,
  SYSTEM_ROLE            NVARCHAR2(50),
  SOS_ID_PK2             INTEGER,
  DTCREATED              DATE,
  DTMODIFIED             DATE,
  ROW_STATUS             NUMBER(1),
  BATCH_UID              NVARCHAR2(256),
  USER_ID                NVARCHAR2(50),
  PASSWD                 VARCHAR2(400 CHAR),
  FIRSTNAME              NVARCHAR2(100),
  MIDDLENAME             NVARCHAR2(100),
  LASTNAME               NVARCHAR2(100),
  OTHERNAME              NVARCHAR2(100),
  SUFFIX                 NVARCHAR2(100),
  GENDER                 CHAR(1 CHAR),
  EDUC_LEVEL             NUMBER(2),
  BIRTHDATE              DATE,
  TITLE                  NVARCHAR2(100),
  STUDENT_ID             NVARCHAR2(100),
  EMAIL                  VARCHAR2(100 CHAR),
  JOB_TITLE              NVARCHAR2(100),
  DEPARTMENT             NVARCHAR2(100),
  COMPANY                NVARCHAR2(100),
  STREET_1               NVARCHAR2(100),
  INSTITUTION_ROLES_PK1  INTEGER,
  STREET_2               NVARCHAR2(100),
  STATE                  NVARCHAR2(50),
  ZIP_CODE               NVARCHAR2(50),
  COUNTRY                NVARCHAR2(50),
  B_PHONE_1              NVARCHAR2(50),
  B_PHONE_2              NVARCHAR2(50),
  H_PHONE_1              NVARCHAR2(50),
  H_PHONE_2              NVARCHAR2(50),
  M_PHONE                NVARCHAR2(50),
  B_FAX                  NVARCHAR2(50),
  H_FAX                  NVARCHAR2(50),
  WEBPAGE                VARCHAR2(100 CHAR),
  COMMERCE_ROLE          INTEGER,
  CDROMDRIVE_PC          CHAR(1 CHAR),
  CDROMDRIVE_MAC         NVARCHAR2(20),
  PUBLIC_IND             CHAR(1 CHAR),
  ADDRESS_IND            CHAR(1 CHAR),
  PHONE_IND              CHAR(1 CHAR),
  WORK_IND               CHAR(1 CHAR),
  EMAIL_IND              CHAR(1 CHAR),
  AVAILABLE_IND          CHAR(1 CHAR),
  LAST_LOGIN_DATE        DATE,
  IM_TYPE                NVARCHAR2(64),
  IM_ACCOUNT             NVARCHAR2(64),
  LOCALE                 VARCHAR2(20 CHAR),
  CLD_ID                 NVARCHAR2(255),
  CLD_AVATAR_URL         NVARCHAR2(512),
  UUID                   NVARCHAR2(32),
  CALENDAR_TYPE          CHAR(2 CHAR),
  WEEK_FIRST_DAY         CHAR(1 CHAR),
  DELETE_FLAG            VARCHAR2(1 CHAR)       DEFAULT 'N',
  INSERT_TIME            DATE                   DEFAULT SYSDATE,
  UPDATE_TIME            DATE                   DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_USERS_S1 IS 'This table contains detailed information on users that are in the system. '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CITY IS 'This is user`s city.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.DATA_SRC_PK1 IS 'This is a reference to the Data Source associated with this user, used for SIS integration.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.SYSTEM_ROLE IS ' This is the user`s primary system role.
                   N = None
                   C = Course Administrator
                   U = Guest
                   BB_LE_ADMIN = Learning Environment Administrator
                   O = Observer
                   R = Support
                   Z = System Administrator
                   H = System Support
                   A = User Administrator
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.SOS_ID_PK2 IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.DTCREATED IS 'This is the datetime when the user is created.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.DTMODIFIED IS 'This is the datetime when the user is last modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.ROW_STATUS IS 'Row status: 0=enabled, 1=undefined/deleted, 2=disabled.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.BATCH_UID IS 'This is Unique user identifier within the database. It is used mostly for SIS integration, representing the user`s id in the external system.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.USER_ID IS 'This is the user name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.PASSWD IS 'This is the encrypted user`s password.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.FIRSTNAME IS 'This is the user`s first name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.MIDDLENAME IS 'This is the user`s middle name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.LASTNAME IS 'This is the user`s last name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.OTHERNAME IS 'This is the user`s other name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.SUFFIX IS 'This is the suffix on the user`s name.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.GENDER IS 'This is the user`s gender.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.EDUC_LEVEL IS ' This is a user`s education level.
                   0 = None
                   8 = K-8
                   12 = High school
                   13 = Freshman
                   14 = Sophomore
                   15 = Junior
                   16 = Senior
                   18 = Graduate School
                   20 = Post Graduate School
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.BIRTHDATE IS 'This is the user`s birth date.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.TITLE IS 'This is the user`s title. E.g. Mr., Dr., etc.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.STUDENT_ID IS ' Student ID as assigned by the institution. Uniqueness is not enforced on this field, it is used to store information only.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.EMAIL IS 'This is the user`s email address.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.JOB_TITLE IS 'This is the user`s job title.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.DEPARTMENT IS 'This is the user`s department.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.COMPANY IS 'This is the user`s company.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.STREET_1 IS 'This the user`s first line street address.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.INSTITUTION_ROLES_PK1 IS 'This is the foreign key referencing Institution_Roles table. This is the user`s primary institutional role.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.STREET_2 IS 'This the user`s second line street address.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.STATE IS 'This is user`s state.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.ZIP_CODE IS 'This is the user`s zip code.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.COUNTRY IS 'This is the user`s country.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.B_PHONE_1 IS 'This is the user`s business phone number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.B_PHONE_2 IS 'This is the user`s secondary business phone number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.H_PHONE_1 IS 'This is the user`s home phone number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.H_PHONE_2 IS 'This is the user`s secondary home phone number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.M_PHONE IS 'This is the user`s mobile phone number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.B_FAX IS 'This is the user`s business fax number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.H_FAX IS 'This is the user`s home fax number.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.WEBPAGE IS 'This is the user`s webpage.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.COMMERCE_ROLE IS 'This field relates to a Blackboard.com value. It is only relevant in the context of Blackboard.com. This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CDROMDRIVE_PC IS 'One character identifying the drive of the CD-ROM drive on the users personal computer. This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CDROMDRIVE_MAC IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.PUBLIC_IND IS 'This is an indicator of whether the user`s information are viewable by nobody, everyone, or classmates.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.ADDRESS_IND IS 'This is a boolean (Y/N) indicator of whether the user`s address should be in the user directory.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.PHONE_IND IS 'This is a boolean (Y/N) indicator of whether the user`s phone number should be in the user directory.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.WORK_IND IS 'This is a boolean (Y/N) indicator of whether the user`s work contact information (company, department, title, phone, fax) will be displayed in the User Directory.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.EMAIL_IND IS 'This is a boolean (Y/N) indicator of whether the user`s email address should be in the user directory.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.AVAILABLE_IND IS 'This is a boolean (Y/N) indicator of whether the user is available or unavailable within the system. Unavailable users cannot log in.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.LAST_LOGIN_DATE IS 'This is the datetime the user last logged on to BB Learn.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.IM_TYPE IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.IM_ACCOUNT IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.LOCALE IS 'This is the locale that the user wishes to view the application in.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CLD_ID IS 'Id of the user`s cloud profile.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CLD_AVATAR_URL IS 'The URL of the user`s cloud avatar img.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.UUID IS 'A unique (generated) identifier for this user.  Sent as the user_id in LTI launches.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.CALENDAR_TYPE IS 'This is user default calendar. GG = Gregorian, HH = Hijri, GH = Gregorian-Hijri, HG = Hijri-Gregorian.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.WEEK_FIRST_DAY IS 'This is the user`s default first day of the week. 0 = Sunday, 1 = Monday, 6 = Saturday'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_USERS_S1.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

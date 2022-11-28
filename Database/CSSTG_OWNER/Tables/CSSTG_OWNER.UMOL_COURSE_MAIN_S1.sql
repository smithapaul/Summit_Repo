DROP TABLE CSSTG_OWNER.UMOL_COURSE_MAIN_S1 CASCADE CONSTRAINTS
/

--
-- UMOL_COURSE_MAIN_S1  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_COURSE_MAIN_S1
(
  BB_SOURCE             VARCHAR2(10 CHAR)       NOT NULL,
  PK1                   INTEGER                 NOT NULL,
  BUTTONSTYLES_PK1      INTEGER,
  CARTRIDGE_PK1         INTEGER,
  CLASSIFICATIONS_PK1   INTEGER,
  DATA_SRC_PK1          INTEGER,
  SOS_ID_PK2            INTEGER,
  DTCREATED             DATE,
  DTMODIFIED            DATE,
  COURSE_NAME           NVARCHAR2(333),
  COURSE_ID             VARCHAR2(100 CHAR),
  LOWER_COURSE_ID       VARCHAR2(100 CHAR),
  ROW_STATUS            NUMBER(1),
  BATCH_UID             NVARCHAR2(256),
  ENROLL_OPTION         CHAR(1 CHAR),
  DURATION              CHAR(1 CHAR),
  PACE                  CHAR(1 CHAR),
  SERVICE_LEVEL         CHAR(1 CHAR),
  ABS_LIMIT             NUMBER,
  SOFT_LIMIT            NUMBER,
  UPLOAD_LIMIT          NUMBER,
  START_DATE            DATE,
  END_DATE              DATE,
  ENROLL_START_DATE     DATE,
  ENROLL_END_DATE       DATE,
  DAYS_OF_USE           NUMBER(6),
  FEE                   NUMBER(11,2),
  ENROLL_ACCESS_CODE    NVARCHAR2(50),
  BANNER_URL            NVARCHAR2(255),
  INSTITUTION_NAME      NVARCHAR2(255),
  REG_LEVEL_IND         CHAR(1 CHAR),
  NAVIGATION_STYLE      VARCHAR2(20 CHAR),
  TEXTCOLOR             VARCHAR2(20 CHAR),
  BACKGROUND_COLOR      VARCHAR2(20 CHAR),
  COLLAPSIBLE_IND       CHAR(1 CHAR),
  ALLOW_GUEST_IND       CHAR(1 CHAR),
  CATALOG_IND           CHAR(1 CHAR),
  LOCKOUT_IND           CHAR(1 CHAR),
  DESC_PAGE_IND         CHAR(1 CHAR),
  AVAILABLE_IND         CHAR(1 CHAR),
  ALLOW_OBSERVER_IND    CHAR(1 CHAR),
  DEFAULT_CONTENT_VIEW  CHAR(1 CHAR),
  LOCALE                VARCHAR2(20 CHAR),
  IS_LOCALE_ENFORCED    CHAR(1 CHAR),
  ASMT_UPGRADE_VERSION  NUMBER,
  ASMT_UPGRADE_FLAGS    NUMBER,
  HONOR_TERM_AVAIL_IND  CHAR(1 CHAR),
  COURSE_THEME_PK1      INTEGER,
  IMPORT_TASK_PK1       INTEGER,
  UUID                  NVARCHAR2(32),
  ULTRA_STATUS          CHAR(1 CHAR),
  COURSE_VIEW_OPTION    CHAR(1 CHAR),
  IS_CLOSED_IND         CHAR(1 CHAR),
  CONVERT_TASK_PK1      INTEGER,
  BANNER_ALT            NVARCHAR2(255),
  COPY_FROM_UUID        NVARCHAR2(32),
  DELETE_FLAG           VARCHAR2(1 CHAR)        DEFAULT 'N',
  INSERT_TIME           DATE                    DEFAULT SYSDATE,
  UPDATE_TIME           DATE                    DEFAULT SYSDATE
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_COURSE_MAIN_S1 IS 'This table contains the basic information for a course. Records are created when a course is created or imported.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BUTTONSTYLES_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].buttonstyles table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.CARTRIDGE_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].cartridge table. It is the course cartridge that is used for the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.CLASSIFICATIONS_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].classifications table. This determines the classification (subject area and the discipline) of the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DATA_SRC_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].data_source table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.SOS_ID_PK2 IS 'This column is deprecated.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DTCREATED IS 'This is the datetime when the course was first created.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DTMODIFIED IS 'This is the datetime when the course was last modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COURSE_NAME IS 'This is the name of the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COURSE_ID IS 'This is the ID of the course. It can be provided by the user.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.LOWER_COURSE_ID IS 'This is the ID of the course in lowercase letters.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ROW_STATUS IS 'Row status: 0=enabled, 1=undefined, 2=disabled.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BATCH_UID IS 'The unique id for the course. For internally created courses it is the same as course_id but for SIS-sourced courses it is set by the SIS feed'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ENROLL_OPTION IS '
            This is the enrollment method that is set for the course.
           S = Self Enrollment
           I = Instructor/ System Administrator
           E = Email Enrollment
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DURATION IS '
           This is to indicate if the duration of the course availability is continuous, date range, x days from enrollment, or term-dictated.
           C = Continuous
           R = Date Range
           D = x days from enrollment
           T = term-dictated (the associated term`s duration controls this course`s duration)
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.PACE IS '
       Deprecated Field: Not used anymore
       I = Instructor Led
       S = Self Paced
       '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.SERVICE_LEVEL IS '
            This is the service_level set for the course.
           F = Full/Free Course
           C = Community (Organization)
           R = Registered Course
           T = Test Drive
           S = System
           L = System
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ABS_LIMIT IS 'Handles the disk quota absolute limit on content. The content in the course may not exceed this limit. Expressed in bytes.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.SOFT_LIMIT IS 'Handles the disk quota soft limit on content. Instructors receive a warning email when this limit is exceeded. Expressed in bytes.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.UPLOAD_LIMIT IS 'No longer used in Learn 9.1.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.START_DATE IS 'This is the datetime that the course starts to be available to access.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.END_DATE IS 'This is the datetime that the course stops to be available to access.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ENROLL_START_DATE IS 'This is the datetime when the enrollment to the course can be started.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ENROLL_END_DATE IS 'This is the datetime when the enrollment to the course ends.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DAYS_OF_USE IS 'Number of days that Students may access the course after enrollment. Useful for self-paced learning.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.FEE IS 'This field is not used'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ENROLL_ACCESS_CODE IS 'This is the access code/password for students to use when enrolling to the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BANNER_URL IS 'This is the URL where the course banner image is stored at. Link to an image that will display at the top of the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.INSTITUTION_NAME IS 'The institution name associated with this course (not used?)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.REG_LEVEL_IND IS 'This field is used for the Blackboard.com service. It has not relevance outside of the Blackboard.com site. This field is not used'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.NAVIGATION_STYLE IS 'This is the navigation menu style that is set for the course. Determines whether the Course Menu uses buttons or text links.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.TEXTCOLOR IS 'This is the text color used for text in the Course Menu.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BACKGROUND_COLOR IS 'This is the background color that is set for the Course Menu.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COLLAPSIBLE_IND IS 'Determines whether or not the Course Menu can be consolidated to show just the top headings or expanded to show subheads. This field is not used. Original intention: This flag determines whether or not users of a course should be allowed to `collapse` (slide into the side) the navigation links which appear on the left hand side of the course page.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ALLOW_GUEST_IND IS 'This is the flag that indicates if guest access is allowed.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.CATALOG_IND IS 'This field is not used'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.LOCKOUT_IND IS 'This field is not used. Original intent: lockout all but administrator user from the course. Indicates if access to the course or organization has been restricted. If set to Y access to the course or organization will be restricted based on the END_DATE and START_DATE.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DESC_PAGE_IND IS 'This field is not used'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.AVAILABLE_IND IS '
           This is the flag that indicates if this course is set to be available.
           If honor_term_avail_ind is true, this flag is ignored and this course`s availability is determined by its
           associated term available_ind.
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ALLOW_OBSERVER_IND IS 'This is the flag that indicates if observers are allowed for this course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DEFAULT_CONTENT_VIEW IS '
           This is the view option that decides how the course content view looks like.
           T = Text Only
           I = Icon Only
           X = Icon and Text
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.LOCALE IS 'The locale (language) in which the course should be delivered - see is_locale_enforced. A null value means `use system default`'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.IS_LOCALE_ENFORCED IS 'Values:Y/N: If Y then the locale will be forced to locale regardless of what the user`s personal preferences have set for their locale'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ASMT_UPGRADE_VERSION IS 'The assessment data has to go through a number of runtime upgrade steps before assessments are usable in a course.  This field records the current upgrade-state of the assessments data within this course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ASMT_UPGRADE_FLAGS IS 'After any import or restore operation the asmt_upgrade_version is set back to 0 and any new data re-upgraded.  There are certain parts of the upgrade which do not have to run in this case (i.e. the upgrade is only required once per course, not after each import operation).  This field is used to store those flags to avoid re-running pieces of the upgrade'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.HONOR_TERM_AVAIL_IND IS 'if Y then use the term for available_ind; if N then use those fields from this table. (Note that duration implicitly affects availability through fields such as startdate and enddate but those are only controlled by the term when course.duration=T)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COURSE_THEME_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].course_theme table. It is the course theme that is used for the course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.IMPORT_TASK_PK1 IS 'The latest queued task pk1 which is import/restore/copy into this course. it is overwritten if multiple ones are queued. This field is transient so not be included in course copy or content exchange operations'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.UUID IS 'A unique (generated) identifier for this course.  Sent as the context_id in LTI launch requests.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.ULTRA_STATUS IS '
           This is the flag indicating if the course is classic, ultra, or ultra preview.  Note that updates to this field are controlled in java code so please do not blindly change it directly.
           Specifically you can only perform these transitions:
           N->C, N->U, N->P, C->U, C->P, P->U, P->C, U->NOTHING. You cannot go back once you pick U
           The NULL value, which represents classic, will be left for all the legacy courses and treated the same as classic
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COURSE_VIEW_OPTION IS '
           This is course view option flag. It`s different from ultra_status flag, this flag control if instructor can change the course view or change directly by admin,
           while ultra_status just indicate the current course view of this course is.
           If null, then the default value depends on the system-wide ultra setting:
           If ultra is ON then null is effectively `I`
           If ultra is OFF then null is effectively `C`
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.IS_CLOSED_IND IS '
           This is the flag indicating if the course is closed for student access.  Even if the course is closed, the course is still listed in the student`s course listing.
           The NULL value will be interpreted as N
         '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.CONVERT_TASK_PK1 IS 'The latest queued task pk1 which is a course conversion. it is overwritten if multiple ones are queued. This field is transient so not be included in course copy or content exchange operations'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.BANNER_ALT IS 'This is the ALT text for the course banner image.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.COPY_FROM_UUID IS 'The source course uuid on copy. We just keep the most recent source course. Sent as the context_id_history in LTI launch requests.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_MAIN_S1.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

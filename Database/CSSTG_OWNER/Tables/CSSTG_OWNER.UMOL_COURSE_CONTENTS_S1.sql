DROP TABLE CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1 CASCADE CONSTRAINTS
/

--
-- UMOL_COURSE_CONTENTS_S1  (Table) 
--
CREATE TABLE CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1
(
  BB_SOURCE              VARCHAR2(10 CHAR)      NOT NULL,
  PK1                    INTEGER                NOT NULL,
  CNTHNDLR_HANDLE        VARCHAR2(65 CHAR),
  DTCREATED              DATE,
  DTMODIFIED             DATE,
  CONTENT_TYPE           VARCHAR2(4 CHAR),
  POSITION               NUMBER,
  FONT_COLOR             VARCHAR2(10 CHAR),
  TEXT_FORMAT_TYPE       CHAR(1 CHAR),
  OFFLINE_NAME           NVARCHAR2(255),
  OFFLINE_PATH           NVARCHAR2(255),
  START_DATE             DATE,
  CRSMAIN_PK1            INTEGER,
  END_DATE               DATE,
  LESSON_IND             CHAR(1 CHAR),
  SEQUENTIAL_IND         CHAR(1 CHAR),
  NEW_WINDOW_IND         CHAR(1 CHAR),
  TRACKING_IND           CHAR(1 CHAR),
  FOLDER_IND             CHAR(1 CHAR),
  DESCRIBE_IND           CHAR(1 CHAR),
  CARTRIDGE_IND          CHAR(1 CHAR),
  AVAILABLE_IND          CHAR(1 CHAR),
  WEB_URL                VARCHAR2(1024 CHAR),
  WEB_URL_HOST           VARCHAR2(255 CHAR),
  ALLOW_GUEST_IND        CHAR(1 CHAR),
  ALLOW_OBSERVER_IND     CHAR(1 CHAR),
  IS_GROUP_CONTENT       CHAR(1 CHAR),
  TITLE                  NVARCHAR2(333),
  PARENT_PK1             INTEGER,
  DATA_VERSION           INTEGER,
  REVIEWABLE_IND         CHAR(1 CHAR),
  VIEW_MODE              CHAR(1 CHAR),
  LINK_REF               VARCHAR2(255 CHAR),
  SAMPLE_CONTENT_IND     CHAR(1 CHAR),
  PARTIALLY_VISIBLE_IND  CHAR(1 CHAR),
  DESCRIPTION            NVARCHAR2(250),
  FOLDER_TYPE            CHAR(1 CHAR),
  COPY_FROM_PK1          INTEGER,
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

COMMENT ON TABLE CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1 IS 'Main table containing information about course contents. (Content.java). Course contents is the repository for all content items in a course.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.BB_SOURCE IS 'This is a Summit column that identifies the source of the Blackboard data (Partitioned).
               UMAMH       = UMass Amherst
               UMBOS       = UMass Boston
               UMDAR       = UMass Dartmouth
               UMLOW       = UMass Lowell
               UMLOW_DAY   = UMass Lowell Day School
               UMOL        = UMass Online
               UMWOR       = UMass Worcester
               '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.CNTHNDLR_HANDLE IS 'Identifies the content handler associated with this piece of content. See content_handlers.handle. The cnthndlr_handle shows the different types of content files, Within a Blackboard course. There are a number of content types available for the instructor to choose from: Plain files, Folders, Assignments, URLs, Learning Units, etc. Each content type available to instructors has a unique cnthndlr_handle value in the course_contents table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DTCREATED IS 'This is the date and/or time at which this event occurred.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DTMODIFIED IS 'This is the date and/or time at which this event modified.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.CONTENT_TYPE IS 'How the content should be rendered. URL=as a URL container (a la ExternalLink), LINK=as a course link, REG=default fashion (a la Course Documents), FILE=as reference to a single file, LRN=NOT USED OR SUPPORTED'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.POSITION IS 'Position of a content on UI, which shows where on the page it will be listed.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.FONT_COLOR IS 'Font color.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.TEXT_FORMAT_TYPE IS 'This is the format of the main_data P=Plain text, H=HTML, S=Smart Text, X=Default'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.OFFLINE_NAME IS 'Not used anymore: Historically the name of the offline content (offline tool not supported anymore)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.OFFLINE_PATH IS 'Not used anymore: Historically the path to the offline content (offline tool not supported anymore)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.START_DATE IS 'The time when the action happened.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.CRSMAIN_PK1 IS 'This refers the primary key of the course_main table.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.END_DATE IS 'The time when the action ended.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.LESSON_IND IS 'Indicates if this is a learning module (Y)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.SEQUENTIAL_IND IS 'Indicates that the user must view the content within this learning module (i.e. only if lesson_ind=Y) sequentially (Y) '
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.NEW_WINDOW_IND IS 'Indicates whether course content appears in a new window.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.TRACKING_IND IS 'Y=Tracking is enabled for this content'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.FOLDER_IND IS 'Y=This is a folder of content; N=this is content itself'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DESCRIBE_IND IS 'Appears to be not used.  Historical: Y=the content contains metadata'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.CARTRIDGE_IND IS 'Y=this content came from a course cartridge and more specifically - it is protected/authorized content that should not be published/exported/etc.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.AVAILABLE_IND IS 'Indicates whether the course content is avaiable.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.WEB_URL IS 'URL associated with this content item.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.WEB_URL_HOST IS 'Hostname from the URL associated with this content item.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.ALLOW_GUEST_IND IS 'Indicates whether a guest is allowed.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.ALLOW_OBSERVER_IND IS 'Indicates whether an observer is allowed.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.IS_GROUP_CONTENT IS 'Indicates whether this is a group content.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.TITLE IS 'Course content title.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.PARENT_PK1 IS 'Course content parent key. This is a foreign key to course_contents.pk1. The parent_pk1 describes the folder that this content item belongs in. If this is a top level content item, then parent_pk1 is null.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DATA_VERSION IS 'This is course contents data version.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.REVIEWABLE_IND IS 'Indicates whether the course content is reviewable.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.VIEW_MODE IS 'THIS is view mode: T = TEXT_ONLY, I = ICON_ONLY, X = TEXT_ICON_ONLY.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.LINK_REF IS 'Used for a building block providers to set an identifying string on content they create that they can tie back to data on their end.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.SAMPLE_CONTENT_IND IS 'Content imported as part of a course structure will be automatically marked as `sample content` which results in an additional css style applied when viewing the content.  A null value for this column == `N` (not sample content)'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.PARTIALLY_VISIBLE_IND IS 'Used to indicate if the content is partially visible, i.e., Title only, to users within an ultra course. NULL means not partially visible as same as `N`.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DESCRIPTION IS 'Course content description.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.FOLDER_TYPE IS 'Used to store the type of folder, only if folder_ind = Y, otherwise it is null and ignored.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.COPY_FROM_PK1 IS 'The source pk1 on course copy. Only keeping the most recent source course. Sent as resource_link_id_history in LTI launches.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.UMOL_COURSE_CONTENTS_S1.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

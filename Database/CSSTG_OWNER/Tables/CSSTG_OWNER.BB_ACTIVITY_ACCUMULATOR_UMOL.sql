DROP TABLE CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL CASCADE CONSTRAINTS
/

--
-- BB_ACTIVITY_ACCUMULATOR_UMOL  (Table) 
--
CREATE TABLE CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL
(
  PK1              INTEGER                      NOT NULL,
  EVENT_TYPE       VARCHAR2(30 CHAR)            NOT NULL,
  USER_PK1         INTEGER,
  COURSE_PK1       INTEGER,
  GROUP_PK1        INTEGER,
  FORUM_PK1        INTEGER,
  INTERNAL_HANDLE  VARCHAR2(255 CHAR),
  CONTENT_PK1      INTEGER,
  DATA             NVARCHAR2(255),
  TIMESTAMP        DATE,
  STATUS           NUMBER,
  SESSION_ID       INTEGER,
  DELETE_FLAG      VARCHAR2(1 CHAR)             DEFAULT 'N',
  INSERT_TIME      DATE                         DEFAULT SYSDATE,
  UPDATE_TIME      DATE                         DEFAULT SYSDATE
)
COMPRESS BASIC
PARTITION BY RANGE (TIMESTAMP)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(  
  PARTITION OLD_DATA VALUES LESS THAN (TO_DATE(' 2020-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-05-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-06-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-07-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-09-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-10-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-11-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2021-12-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-05-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-06-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-07-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-09-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-10-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-11-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC,  
  PARTITION VALUES LESS THAN (TO_DATE(' 2022-12-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN'))
    COMPRESS BASIC
)
/

COMMENT ON TABLE CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL IS 'This is the table that tracks user activity. Almost every page a user visits in your system gets recorded here. It is like your apache access logs but with better user tracking. '
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.PK1 IS 'This is the surrogate primary key for the table.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.EVENT_TYPE IS 'This is the type of activity. Groups the event based on what occurred. For example, TAB_ACCESS (page view), PAGE_ACCESS (page view, Triggered when a page, other than a course, organization, content, module, or tab is accessed on the system. The name of the navigation item will be returned.), MODULE_ACCESS, LOGIN_ATTEMPT, LOGOUT, SESSION_TIMEOUT, COURSE_ACCESS (Page view, Triggered when a course frameset is loaded, or when a page is accessed in a course. The name of the navigation item will be returned.), SESSION_INIT (Triggered when a session is initialized), etc. The full list of values is in the TrackingEvent.Type enumueration.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.USER_PK1 IS 'This is a foreign key referencing the primary key of the [AS_CORE].users table to indicate the owner of the activity. This can be NULL if the user cannot be determined. If the user has logged in, then there activity is tied to their user_pk1.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.COURSE_PK1 IS 'This is a foreign ley referencing the primary key of the [AS_CORE].course_main table to indicate the Course the activity is occuring in. This will be NULL if the activity occurs outside a Course. If the user has logged in, then there activity is tied to their user_pk1.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.GROUP_PK1 IS 'This is a foreign key referencing the primary key of the group table to indicate the Course Group this activity belongs to. This will be NULL if the activity is not Course Group related.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.FORUM_PK1 IS 'This is a foreign key referencing the primary key of the forum table to indicate the Forum this activity is taking place in. Forum activity can take place within a Course or Institution Discussion Board. This will be NULL if the activity takes place outside a Forum.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.INTERNAL_HANDLE IS 'This is a foreign key referencing the internal_handle column of the [AS_CORE].navigation_item table. This is tracked whenever a user accesses a page in the the Academic Suite (eg. When either a PAGE_ACCESS or COURSE_ACCESS event occurs). This will be NULL for events other than PAGE_ACCESS and COURSE_ACCESS. Internal system identifier for the event. This field corresponds to the unique ID in the NAVIGATION_ITEM table. Not all page requests correspond to a navigation item, so this field may often appear blank. If this field is empty, check the CONTENT_PK1 field. If the page accessed displayed content, the primary key for the piece of content will appear in that field.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.CONTENT_PK1 IS 'This is a foreign key referencing the primary key of the course_contents table to indicate the Course Content this activity is associated with. This will be NULL if the activity is not Course Content related. If the user has logged in, then there activity is tied to their user_pk1. This field is populated when the EVENT_TYPE is CONTENT_ACCESS.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.DATA IS 'This contains any free-form text data associated with the event. Data related to the event. This field provides additional information on the event. Each event uses this field, if at all, in different ways. In many instances, the data included in this field is a value that is easily deduced. In some instances, the value in the data field is a number, such as _1_1 or _29_1. These are associated with an EVENT_TYPE of TAB_ACCESS or MODULE_ACCESS. The first number is an ID that references a tab or a module (the second number can be ignored). The ID number that refers to a tab can be found in the user interface by scrolling over the tab. The ID number appears as part of the URL in the status bar.The ID number that refers to a module can be found in the same manner. Scroll over the Maximize, Minimize, or Edit button associated with a module. The ID number will appear as part of the URL in the status bar. Please note that the Detach Module button returns a different ID number and should be ignored.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.TIMESTAMP IS 'This is the date and/or time at which this event occurred.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.STATUS IS 'This indicates the status of the event. Value values are success (1) or failure (0). The values come from the TrackingEvent.Status enumeration.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.SESSION_ID IS 'This is a reference to the primary key of the user session associated with this activity. Identifies the user session that initiated the action. A session is simply a browser connection to the system launched from an end-user machine.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.DELETE_FLAG IS 'This is a Summit Y/N flag that indicates that this row is no longer in the source data.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.INSERT_TIME IS 'This is the date and time that this row was inserted into the Summit database.'
/

COMMENT ON COLUMN CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMOL.UPDATE_TIME IS 'This is the date and time that this row was last updated in the Summit database.'
/

DROP TABLE CSSTG_OWNER.PS_CLASS_MTG_PAT CASCADE CONSTRAINTS
/

--
-- PS_CLASS_MTG_PAT  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_CLASS_MTG_PAT
(
  CRSE_ID             VARCHAR2(6 BYTE)          NOT NULL,
  CRSE_OFFER_NBR      INTEGER                   NOT NULL,
  STRM                VARCHAR2(4 BYTE)          NOT NULL,
  SESSION_CODE        VARCHAR2(3 BYTE)          NOT NULL,
  CLASS_SECTION       VARCHAR2(4 BYTE)          NOT NULL,
  CLASS_MTG_NBR       INTEGER                   NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  FACILITY_ID         VARCHAR2(10 BYTE)         NOT NULL,
  MEETING_TIME_START  DATE,
  MEETING_TIME_END    DATE,
  MON                 VARCHAR2(1 BYTE)          NOT NULL,
  TUES                VARCHAR2(1 BYTE)          NOT NULL,
  WED                 VARCHAR2(1 BYTE)          NOT NULL,
  THURS               VARCHAR2(1 BYTE)          NOT NULL,
  FRI                 VARCHAR2(1 BYTE)          NOT NULL,
  SAT                 VARCHAR2(1 BYTE)          NOT NULL,
  SUN                 VARCHAR2(1 BYTE)          NOT NULL,
  START_DT            DATE,
  END_DT              DATE,
  CRS_TOPIC_ID        INTEGER                   NOT NULL,
  DESCR               VARCHAR2(30 BYTE)         NOT NULL,
  STND_MTG_PAT        VARCHAR2(4 BYTE)          NOT NULL,
  PRINT_TOPIC_ON_XCR  VARCHAR2(1 BYTE)          NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

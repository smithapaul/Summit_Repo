DROP TABLE CSMRT_OWNER.PS_D_DAY CASCADE CONSTRAINTS
/

--
-- PS_D_DAY  (Table) 
--
CREATE TABLE CSMRT_OWNER.PS_D_DAY
(
  DAY_SID          INTEGER                      NOT NULL,
  DAY_DT           DATE,
  DAY_NUM          INTEGER                      NOT NULL,
  DAY_DESCR        VARCHAR2(30 CHAR)            NOT NULL,
  DAY_ABBR         VARCHAR2(3 CHAR)             NOT NULL,
  DAY_JULIAN       INTEGER                      NOT NULL,
  DAY_JULIAN_YYY   INTEGER                      NOT NULL,
  DAY_WK_NUM       INTEGER                      NOT NULL,
  DAY_MTH_NUM      INTEGER                      NOT NULL,
  DAY_YR_NUM       INTEGER                      NOT NULL,
  WEEK_SID         INTEGER                      NOT NULL,
  WEEK_NUM         INTEGER                      NOT NULL,
  WEEK_DESCR       VARCHAR2(30 CHAR)            NOT NULL,
  WEEK_MTH_NUM     INTEGER                      NOT NULL,
  WEEK_YR_NUM      INTEGER                      NOT NULL,
  MONTH_SID        INTEGER                      NOT NULL,
  MONTH_NUM        INTEGER                      NOT NULL,
  MONTH_DESCR      VARCHAR2(30 CHAR)            NOT NULL,
  MONTH_ABBR       VARCHAR2(3 CHAR)             NOT NULL,
  MONTH_QTR_NUM    INTEGER                      NOT NULL,
  MONTH_YR_NUM     INTEGER                      NOT NULL,
  QUARTER_SID      INTEGER                      NOT NULL,
  QUARTER_NUM      INTEGER                      NOT NULL,
  QUARTER_DESCR    VARCHAR2(30 CHAR)            NOT NULL,
  QUARTER_ABBR     VARCHAR2(5 CHAR)             NOT NULL,
  QUARTER_YR_NUM   INTEGER                      NOT NULL,
  YEAR_SID         INTEGER                      NOT NULL,
  YEAR_NUM         INTEGER                      NOT NULL,
  YEAR_DESCR       VARCHAR2(30 CHAR)            NOT NULL,
  FIRSTDAYWK_FLG   VARCHAR2(1 CHAR)             NOT NULL,
  LASTDAYWK_FLG    VARCHAR2(1 CHAR)             NOT NULL,
  FIRSTDAYMTH_FLG  VARCHAR2(1 CHAR)             NOT NULL,
  LASTDAYMTH_FLG   VARCHAR2(1 CHAR)             NOT NULL,
  FIRSTDAYQTR_FLG  VARCHAR2(1 CHAR)             NOT NULL,
  LASTDAYQTR_FLG   VARCHAR2(1 CHAR)             NOT NULL,
  FIRSTDAYYR_FLG   VARCHAR2(1 CHAR)             NOT NULL,
  LASTDAYYR_FLG    VARCHAR2(1 CHAR)             NOT NULL,
  DAY_WEEKEND_FLG  VARCHAR2(1 CHAR)             NOT NULL,
  LOAD_ERROR       VARCHAR2(1 CHAR)             NOT NULL,
  DATA_ORIGIN      VARCHAR2(1 CHAR)             NOT NULL,
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE,
  BATCH_SID        NUMBER(10)                   NOT NULL
)
NOCOMPRESS
/


ALTER TABLE CSMRT_OWNER.PS_D_DAY ADD (
  CONSTRAINT PK_PS_D_DAY
  PRIMARY KEY
  (DAY_SID)
  RELY
  DISABLE NOVALIDATE)
/

CREATE TABLE PS_D_TERM
(
  TERM_SID           INTEGER,
  INSTITUTION_CD     VARCHAR2(5 CHAR),
  ACAD_CAR_CD        VARCHAR2(4 CHAR),
  TERM_CD            VARCHAR2(4 CHAR),
  SRC_SYS_ID         VARCHAR2(5 CHAR),
  TERM_SD            VARCHAR2(10 CHAR),
  TERM_LD            VARCHAR2(30 CHAR),
  TERM_CD_DESC       VARCHAR2(50 CHAR),
  INSTITUTION_SID    INTEGER,
  ACAD_CAR_SID       INTEGER,
  ACAD_YR_SID        INTEGER,
  TERM_BEGIN_DT      DATE,
  TERM_END_DT        DATE,
  EFF_START_DT       DATE,
  EFF_END_DT         DATE,
  CURRENT_TERM_FLG   VARCHAR2(1 CHAR),
  AID_YEAR           VARCHAR2(4 CHAR),
  INSTRCTN_WEEK_NUM  INTEGER,
  SIXTY_PCT_DT       DATE,
  PREV_TERM          VARCHAR2(4 CHAR),
  PREV_TERM_2        VARCHAR2(4 CHAR),
  NEXT_TERM          VARCHAR2(4 CHAR),
  NEXT_TERM_2        VARCHAR2(4 CHAR),
  PREV_FALL          VARCHAR2(4 CHAR),
  PREV_WINTER        VARCHAR2(4 CHAR),
  PREV_SPRING        VARCHAR2(4 CHAR),
  PREV_SUMMER        VARCHAR2(4 CHAR),
  PREV_SUMMER_2      VARCHAR2(4 CHAR),
  NEXT_FALL          VARCHAR2(4 CHAR),
  NEXT_WINTER        VARCHAR2(4 CHAR),
  NEXT_SPRING        VARCHAR2(4 CHAR),
  NEXT_SUMMER        VARCHAR2(4 CHAR),
  NEXT_SUMMER_2      VARCHAR2(4 CHAR),
  DATA_ORIGIN        VARCHAR2(1 CHAR),
  CREATED_EW_DTTM    DATE,
  LASTUPD_EW_DTTM    DATE
)
NOLOGGING 
NOCOMPRESS 
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

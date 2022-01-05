CREATE TABLE PS_D_REPEAT
(
  REPEAT_SID        INTEGER,
  SETID             VARCHAR2(5 CHAR),
  REPEAT_SCHEME_CD  VARCHAR2(4 CHAR),
  REPEAT_CD         VARCHAR2(4 CHAR),
  SRC_SYS_ID        VARCHAR2(5 CHAR),
  EFFDT             DATE,
  REPEAT_SD         VARCHAR2(10 CHAR),
  REPEAT_LD         VARCHAR2(30 CHAR),
  REPEAT_FD         VARCHAR2(50 CHAR),
  REPEAT_SCHEME_SD  VARCHAR2(10 CHAR),
  REPEAT_SCHEME_LD  VARCHAR2(30 CHAR),
  DATA_ORIGIN       VARCHAR2(1 CHAR),
  CREATED_EW_DTTM   DATE,
  LASTUPD_EW_DTTM   DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

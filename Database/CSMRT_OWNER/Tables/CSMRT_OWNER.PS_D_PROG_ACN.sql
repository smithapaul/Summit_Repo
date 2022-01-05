CREATE TABLE PS_D_PROG_ACN
(
  PROG_ACN_SID      INTEGER,
  SETID             VARCHAR2(5 CHAR),
  PROG_ACN_CD       VARCHAR2(4 CHAR),
  SRC_SYS_ID        VARCHAR2(5 CHAR),
  EFFDT             DATE,
  EFF_STAT_CD       VARCHAR2(1 CHAR),
  PROG_ACN_SD       VARCHAR2(10 CHAR),
  PROG_ACN_LD       VARCHAR2(30 CHAR),
  PROG_ACN_CD_DESC  VARCHAR2(50 CHAR),
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

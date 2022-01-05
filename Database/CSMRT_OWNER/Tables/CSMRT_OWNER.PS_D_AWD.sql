CREATE TABLE PS_D_AWD
(
  AWD_SID          INTEGER,
  INSTITUTION_CD   VARCHAR2(5 CHAR),
  AWD_CD           VARCHAR2(6 CHAR),
  SRC_SYS_ID       VARCHAR2(5 CHAR),
  EFFDT            DATE,
  EFF_STAT_CD      VARCHAR2(1 CHAR),
  AWD_SD           VARCHAR2(10 CHAR),
  AWD_LD           VARCHAR2(30 CHAR),
  AWD_FD           VARCHAR2(50 CHAR),
  INT_EXT_CD       VARCHAR2(1 CHAR),
  INT_EXT_SD       VARCHAR2(10 CHAR),
  INT_EXT_LD       VARCHAR2(30 CHAR),
  GRANTOR_NM       VARCHAR2(20 CHAR),
  DATA_ORIGIN      VARCHAR2(1 CHAR),
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

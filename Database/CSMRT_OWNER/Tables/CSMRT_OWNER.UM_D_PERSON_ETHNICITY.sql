CREATE TABLE UM_D_PERSON_ETHNICITY
(
  PERSON_ID           VARCHAR2(11 CHAR),
  REG_REGION          VARCHAR2(5 CHAR),
  ETHNIC_GRP_CD       VARCHAR2(8 CHAR),
  SRC_SYS_ID          VARCHAR2(5 CHAR),
  PERSON_SID          INTEGER,
  ETHNIC_GRP_SID      INTEGER,
  PRIMARY_FLAG        VARCHAR2(1 CHAR),
  HISP_LATINO_FLG     VARCHAR2(1 CHAR),
  IPEDS_FLG           VARCHAR2(1 CHAR),
  ETHNIC_PCT_NUMRATR  INTEGER,
  ETHNIC_PCT_DENMRTR  INTEGER,
  LASTUPDDTTM         DATE,
  LASTUPDOPRID        VARCHAR2(30 CHAR),
  DATA_ORIGIN         VARCHAR2(1 CHAR),
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

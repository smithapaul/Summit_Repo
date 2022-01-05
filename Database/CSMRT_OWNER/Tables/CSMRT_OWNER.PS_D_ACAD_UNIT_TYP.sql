CREATE TABLE PS_D_ACAD_UNIT_TYP
(
  ACAD_UNIT_TYPE_SID  INTEGER,
  ACAD_UNIT_TYPE_ID   VARCHAR2(4 CHAR),
  SRC_SYS_ID          VARCHAR2(5 CHAR),
  ACAD_UNIT_TYPE_SD   VARCHAR2(10 CHAR),
  ACAD_UNIT_TYPE_LD   VARCHAR2(30 CHAR),
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

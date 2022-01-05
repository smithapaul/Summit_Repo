CREATE TABLE UM_D_PERSON_NAME
(
  PERSON_ID         VARCHAR2(11 BYTE),
  NAME_TYPE         VARCHAR2(4 BYTE),
  SRC_SYS_ID        VARCHAR2(5 BYTE),
  EFFDT             DATE,
  EFF_STATUS        VARCHAR2(1 BYTE),
  PERSON_SID        INTEGER,
  NAME              VARCHAR2(50 BYTE),
  NAME_INITIALS     VARCHAR2(6 BYTE),
  NAME_PREFIX       VARCHAR2(4 BYTE),
  NAME_SUFFIX       VARCHAR2(15 BYTE),
  NAME_TITLE        VARCHAR2(30 BYTE),
  LAST_NAME_SRCH    VARCHAR2(30 BYTE),
  FIRST_NAME_SRCH   VARCHAR2(30 BYTE),
  LAST_NAME         VARCHAR2(30 BYTE),
  FIRST_NAME        VARCHAR2(30 BYTE),
  MIDDLE_NAME       VARCHAR2(30 BYTE),
  PREF_FIRST_NAME   VARCHAR2(30 BYTE),
  NAME_DISPLAY      VARCHAR2(50 BYTE),
  NAME_FORMAL       VARCHAR2(60 BYTE),
  LAST_NAME_FORMER  VARCHAR2(30 BYTE),
  NAME_FORMER       VARCHAR2(50 BYTE),
  LASTUPDDTTM       DATE,
  LASTUPDOPRID      VARCHAR2(30 BYTE),
  NAME_ORDER        INTEGER,
  AKA_ORDER         INTEGER,
  CPS_ORDER         INTEGER,
  DEG_ORDER         INTEGER,
  PRF_ORDER         INTEGER,
  PRI_ORDER         INTEGER,
  DATA_ORIGIN       VARCHAR2(1 BYTE),
  CREATED_EW_DTTM   DATE,
  LASTUPD_EW_DTTM   DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

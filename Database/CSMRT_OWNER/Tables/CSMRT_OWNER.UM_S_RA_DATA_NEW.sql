CREATE TABLE UM_S_RA_DATA_NEW
(
  RUN_DT              DATE,
  INSTITUTION_CD      VARCHAR2(5 BYTE),
  TERM_CD             VARCHAR2(4 BYTE),
  ADM_APPL_NBR        VARCHAR2(8 BYTE),
  SRC_SYS_ID          VARCHAR2(5 BYTE),
  EFFDT_START         DATE,
  EFFDT_END           DATE,
  ACAD_CAR_CD         VARCHAR2(4 BYTE),
  ADMIT_TYPE_GRP      VARCHAR2(30 CHAR),
  ADMIT_TYPE_ID       VARCHAR2(3 CHAR),
  APPL_CNTR_ID        VARCHAR2(4 CHAR),
  ACAD_PROG_CD        VARCHAR2(5 BYTE),
  ACAD_PLAN_CD        VARCHAR2(10 BYTE),
  ACAD_SPLAN_CD       VARCHAR2(10 BYTE),
  EDU_LVL_CTGRY       VARCHAR2(30 BYTE),
  EDU_LVL_CD          VARCHAR2(2 BYTE),
  PROG_STAT_CD        VARCHAR2(4 BYTE),
  PROG_ACN_CD         VARCHAR2(4 CHAR),
  PROG_ACN_RSN_CD     VARCHAR2(4 CHAR),
  ACTION_DT           DATE,
  APPL_CNT            INTEGER,
  APPL_COMPLETE_CNT   INTEGER,
  ADMIT_CNT           INTEGER,
  DENY_CNT            INTEGER,
  WAIT_CNT            INTEGER,
  DEPOSIT_CNT         INTEGER,
  MATRIC_CNT          INTEGER,
  ENROLL_CNT          INTEGER,
  PERSON_ID           VARCHAR2(11 BYTE),
  GENDER_CD           VARCHAR2(1 BYTE),
  AGE                 INTEGER,
  COUNTRY             VARCHAR2(3 BYTE),
  CITIZENSHIP_STATUS  VARCHAR2(1 BYTE),
  ETHNIC_GRP_FED_CD   VARCHAR2(8 BYTE),
  ETHNIC_GRP_ST_CD    VARCHAR2(8 BYTE),
  RSDNCY_ID           VARCHAR2(5 BYTE),
  TUITION_RSDNCY_ID   VARCHAR2(5 BYTE),
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

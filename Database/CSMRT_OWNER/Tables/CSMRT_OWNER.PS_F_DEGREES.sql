CREATE TABLE PS_F_DEGREES
(
  PERSON_ID           VARCHAR2(11 BYTE),
  DEGREE_NBR          INTEGER,
  ACAD_PLAN_CD        VARCHAR2(10 BYTE),
  ACAD_SPLAN_CD       VARCHAR2(10 BYTE),
  SRC_SYS_ID          VARCHAR2(5 BYTE),
  INSTITUTION_CD      VARCHAR2(5 BYTE),
  ACAD_CAR_CD         VARCHAR2(8 BYTE),
  DEG_CD              VARCHAR2(8 BYTE),
  INSTITUTION_SID     INTEGER,
  ACAD_CAR_SID        INTEGER,
  PERSON_SID          INTEGER,
  DEG_SID             INTEGER,
  ACAD_PLAN_SID       INTEGER,
  ACAD_SPLAN_SID      INTEGER,
  ACAD_DEGR_STAT_SID  INTEGER,
  ACAD_PLAN_CAR_SID   INTEGER,
  COMPL_TERM_SID      INTEGER,
  HONORS_PREFIX_SID   INTEGER,
  HONORS_SUFFIX_SID   INTEGER,
  PLN_HONRS_PREF_SID  INTEGER,
  PLN_HONRS_SUFF_SID  INTEGER,
  SPLN_HNRS_PREF_SID  INTEGER,
  SPLN_HNRS_SUFF_SID  INTEGER,
  CLASS_RANK_NBR      INTEGER,
  CLASS_RANK_TOT      INTEGER,
  CONF_DT             DATE,
  DEGR_STAT_DT        DATE,
  GPA_DEGREE          NUMBER(9,3),
  GPA_PLAN            NUMBER(9,3),
  PLN_CLASS_RANK_NBR  INTEGER,
  PLN_CLASS_RANK_TOT  INTEGER,
  PLAN_DEGR_STATUS    VARCHAR2(1 CHAR),
  PLN_DEG_ST_DT       DATE,
  PLAN_DIPLOMA_DESCR  VARCHAR2(100 CHAR),
  PLAN_OVERRIDE_FLG   VARCHAR2(1 CHAR),
  PLAN_SEQUENCE       INTEGER,
  PLAN_TRNSCR_DESCR   VARCHAR2(100 CHAR),
  SPLAN_DIPLOMA_DESC  VARCHAR2(100 CHAR),
  SPLAN_OVERRIDE_FLG  VARCHAR2(1 CHAR),
  SPLAN_SEQUENCE      INTEGER,
  SPLAN_TRNSCR_DESCR  VARCHAR2(100 CHAR),
  STDNT_CAR_NBR       INTEGER,
  DEGREE_COUNT_AWD    INTEGER,
  DEGREE_COUNT_RVK    INTEGER,
  DEGREE_COUNT        INTEGER,
  LOAD_ERROR          VARCHAR2(1 CHAR),
  DATA_ORIGIN         VARCHAR2(1 CHAR),
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

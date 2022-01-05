CREATE TABLE UM_F_PRSPCT_RFRL_AGG
(
  PRSPCT_CAR_SID      INTEGER,
  PRSPCT_PROG_SID     INTEGER,
  PRSPCT_PLAN_SID     INTEGER,
  PRSPCT_SPLAN_SID    INTEGER,
  RECRT_CNTR_SID      INTEGER,
  RFRL_DTL_SID        INTEGER,
  RFRL_DT             DATE,
  RFRL_DT_SID         INTEGER,
  SRC_SYS_ID          VARCHAR2(5 BYTE),
  INSTITUTION_CD      VARCHAR2(5 BYTE),
  INSTITUTION_SID     INTEGER,
  PERSON_SID          INTEGER,
  RSDNCY_SID          INTEGER,
  ADM_RSDNCY_SID      INTEGER,
  FA_FED_RSDNCY_SID   INTEGER,
  FA_ST_RSDNCY_SID    INTEGER,
  TUITION_RSDNCY_SID  INTEGER,
  ADMIT_TERM          VARCHAR2(4 BYTE),
  ADM_RECR_CTR        VARCHAR2(4 BYTE),
  UM_ADM_REC_NBR      VARCHAR2(15 BYTE),
  APPL_PROG_FLAG      VARCHAR2(4 BYTE),
  INIT_RFRL_ORDER     INTEGER,
  LAST_RFRL_ORDER     INTEGER,
  PRSPCT_CNT          INTEGER,
  APPL_CNT            INTEGER,
  APPL_COMPLETE_CNT   INTEGER,
  ADMIT_CNT           INTEGER,
  DENY_CNT            INTEGER,
  DEPOSIT_CNT         INTEGER,
  ENROLL_CNT          INTEGER
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

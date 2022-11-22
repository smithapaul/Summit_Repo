DROP TABLE CSMRT_OWNER.UM_F_PRSPCT_RFRL_AGG CASCADE CONSTRAINTS
/

--
-- UM_F_PRSPCT_RFRL_AGG  (Table) 
--
CREATE TABLE CSMRT_OWNER.UM_F_PRSPCT_RFRL_AGG
(
  PRSPCT_CAR_SID      INTEGER                   NOT NULL,
  PRSPCT_PROG_SID     INTEGER                   NOT NULL,
  PRSPCT_PLAN_SID     INTEGER                   NOT NULL,
  PRSPCT_SPLAN_SID    INTEGER                   NOT NULL,
  RECRT_CNTR_SID      INTEGER                   NOT NULL,
  RFRL_DTL_SID        INTEGER                   NOT NULL,
  RFRL_DT             DATE                      NOT NULL,
  RFRL_DT_SID         INTEGER                   NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  INSTITUTION_CD      VARCHAR2(5 BYTE)          NOT NULL,
  INSTITUTION_SID     INTEGER                   NOT NULL,
  PERSON_SID          INTEGER                   NOT NULL,
  RSDNCY_SID          INTEGER                   NOT NULL,
  ADM_RSDNCY_SID      INTEGER                   NOT NULL,
  FA_FED_RSDNCY_SID   INTEGER                   NOT NULL,
  FA_ST_RSDNCY_SID    INTEGER                   NOT NULL,
  TUITION_RSDNCY_SID  INTEGER                   NOT NULL,
  ADMIT_TERM          VARCHAR2(4 BYTE)          NOT NULL,
  ADM_RECR_CTR        VARCHAR2(4 BYTE)          NOT NULL,
  UM_ADM_REC_NBR      VARCHAR2(15 BYTE)         NOT NULL,
  APPL_PROG_FLAG      VARCHAR2(4 BYTE)          NOT NULL,
  INIT_RFRL_ORDER     INTEGER                   NOT NULL,
  LAST_RFRL_ORDER     INTEGER                   NOT NULL,
  PRSPCT_CNT          INTEGER                   NOT NULL,
  APPL_CNT            INTEGER                   NOT NULL,
  APPL_COMPLETE_CNT   INTEGER                   NOT NULL,
  ADMIT_CNT           INTEGER                   NOT NULL,
  DENY_CNT            INTEGER                   NOT NULL,
  DEPOSIT_CNT         INTEGER                   NOT NULL,
  ENROLL_CNT          INTEGER                   NOT NULL
)
COMPRESS BASIC
/

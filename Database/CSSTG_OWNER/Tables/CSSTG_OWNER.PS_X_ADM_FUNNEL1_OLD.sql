DROP TABLE CSSTG_OWNER.PS_X_ADM_FUNNEL1_OLD CASCADE CONSTRAINTS
/

--
-- PS_X_ADM_FUNNEL1_OLD  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_X_ADM_FUNNEL1_OLD
(
  PERSON_SID          INTEGER                   NOT NULL,
  ACAD_CAR_SID        NUMBER(10)                NOT NULL,
  INSTITUTION_SID     NUMBER(10)                NOT NULL,
  ADMIT_TERM_SID      NUMBER(10)                NOT NULL,
  ACAD_PROG_SID       NUMBER(10)                NOT NULL,
  ACAD_PLAN_SID       NUMBER(10)                NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  PSPT_ADMIT_TRM_SID  INTEGER                   NOT NULL,
  APPL_ADMIT_TRM_SID  INTEGER                   NOT NULL,
  PSPT_ADMIT_TYP_SID  INTEGER                   NOT NULL,
  APPL_ADMIT_TYP_SID  INTEGER                   NOT NULL,
  PSPT_CAMPUS_SID     INTEGER                   NOT NULL,
  APPL_CAMPUS_SID     INTEGER                   NOT NULL,
  PSPT_ACAD_LOAD_SID  INTEGER                   NOT NULL,
  APPL_ACAD_LOAD_SID  INTEGER                   NOT NULL,
  ACAD_LVL_SID        NUMBER(10)                NOT NULL,
  RECRT_STAT_SID      NUMBER(10)                NOT NULL,
  RECRT_STAT_DT_SID   NUMBER(10)                NOT NULL,
  RFRL_SRC_SID        NUMBER(10)                NOT NULL,
  RECRT_CNTR_SID      NUMBER(10)                NOT NULL,
  PSPT_ACAD_PROG_SID  INTEGER                   NOT NULL,
  PSPT_ACAD_PLAN_SID  INTEGER                   NOT NULL,
  PRG_RECRT_STAT_SID  INTEGER                   NOT NULL,
  APPL_ACAD_PROG_SID  INTEGER                   NOT NULL,
  APPL_ACAD_PLAN_SID  INTEGER                   NOT NULL,
  LST_SCHL_ATTND_SID  NUMBER(10)                NOT NULL,
  REGION_CS_SID       NUMBER(10)                NOT NULL,
  APPL_CNTR_SID       NUMBER(10)                NOT NULL,
  PROG_STAT_SID       NUMBER(10)                NOT NULL,
  PROG_ACN_SID        NUMBER(10)                NOT NULL,
  PROG_ACN_RSN_SID    NUMBER(10)                NOT NULL,
  PROG_ACN_DT_SID     NUMBER(10)                NOT NULL,
  APPL_ON_FILE_FLG    VARCHAR2(1 BYTE)          NOT NULL,
  ADM_APPL_NBR        VARCHAR2(8 BYTE)          NOT NULL,
  ACCEPTED_FLG        VARCHAR2(1 BYTE)          NOT NULL,
  PROSPECT_CNT        INTEGER                   NOT NULL,
  APPLCNT_CNT         INTEGER                   NOT NULL,
  ADMIT_CNT           INTEGER                   NOT NULL,
  CONFIRM_CNT         INTEGER                   NOT NULL,
  ENROLL_CNT          INTEGER                   NOT NULL,
  EMPLID              VARCHAR2(11 BYTE)         NOT NULL,
  ACAD_CAREER         VARCHAR2(4 BYTE)          NOT NULL,
  INSTITUTION         VARCHAR2(5 BYTE)          NOT NULL,
  ADMIT_TERM          VARCHAR2(4 BYTE)          NOT NULL,
  ACAD_PROG           VARCHAR2(5 BYTE)          NOT NULL,
  ACAD_PLAN           VARCHAR2(10 BYTE)         NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

DROP TABLE CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT CASCADE CONSTRAINTS
/

--
-- UM_F_ADM_APPL_CLASS_ENRLMT  (Table) 
--
CREATE TABLE CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT
(
  ADM_APPL_SID            INTEGER               NOT NULL,
  SESSION_SID             INTEGER               NOT NULL,
  PERSON_SID              INTEGER               NOT NULL,
  CLASS_NUM               NUMBER(10)            NOT NULL,
  SRC_SYS_ID              VARCHAR2(5 BYTE),
  INSTITUTION_SID         INTEGER               NOT NULL,
  INSTITUTION_CD          VARCHAR2(5 BYTE),
  ACAD_CAR_SID            INTEGER               NOT NULL,
  TERM_SID                INTEGER               NOT NULL,
  CLASS_SID               INTEGER               NOT NULL,
  CLASS_MTG_PAT_SID_P1    INTEGER               NOT NULL,
  CLASS_MTG_PAT_SID_P2    INTEGER               NOT NULL,
  ENRLMT_REAS_SID         INTEGER               NOT NULL,
  ENRLMT_STAT_SID         INTEGER               NOT NULL,
  GRADE_SID               INTEGER               NOT NULL,
  PRI_CLASS_INSTRCTR_SID  INTEGER               NOT NULL,
  REPEAT_SID              INTEGER               NOT NULL,
  ENRL_ADD_DT             DATE,
  ENRL_DROP_DT            DATE,
  ENRLMT_STAT_DT          DATE,
  GRADE_DT                DATE,
  GRADE_BASIS_DT          DATE,
  REPEAT_DT               DATE,
  REPEAT_FLG              VARCHAR2(1 BYTE),
  CLASS_CD                NUMBER,
  CLASS_SECTION_CD        VARCHAR2(4 BYTE),
  GRADE_PTS               NUMBER(9,3),
  BILLING_UNIT            NUMBER(5,2),
  TAKEN_UNIT              NUMBER(5,2),
  PRGRS_UNIT              NUMBER(5,2),
  ERN_UNIT                NUMBER(5,2),
  CE_CREDITS              NUMBER,
  CE_FTE                  NUMBER,
  DAY_CREDITS             NUMBER,
  DAY_FTE                 NUMBER,
  ENROLL_CNT              NUMBER,
  DROP_CNT                NUMBER,
  WAIT_CNT                NUMBER,
  IFTE_CNT                NUMBER
)
COMPRESS BASIC
/


ALTER TABLE CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT ADD (
  CONSTRAINT PK_UM_F_ADM_APPL_CLASS_ENRLMT
  PRIMARY KEY
  (ADM_APPL_SID, SESSION_SID, PERSON_SID, CLASS_NUM, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/

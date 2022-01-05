CREATE TABLE UM_F_ADM_APPL_CLASS_ENRLMT
(
  ADM_APPL_SID            INTEGER,
  SESSION_SID             INTEGER,
  PERSON_SID              INTEGER,
  CLASS_NUM               NUMBER(10),
  SRC_SYS_ID              VARCHAR2(5 BYTE),
  INSTITUTION_SID         INTEGER,
  INSTITUTION_CD          VARCHAR2(5 BYTE),
  ACAD_CAR_SID            INTEGER,
  TERM_SID                INTEGER,
  CLASS_SID               INTEGER,
  CLASS_MTG_PAT_SID_P1    INTEGER,
  CLASS_MTG_PAT_SID_P2    INTEGER,
  ENRLMT_REAS_SID         INTEGER,
  ENRLMT_STAT_SID         INTEGER,
  GRADE_SID               INTEGER,
  PRI_CLASS_INSTRCTR_SID  INTEGER,
  REPEAT_SID              INTEGER,
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
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

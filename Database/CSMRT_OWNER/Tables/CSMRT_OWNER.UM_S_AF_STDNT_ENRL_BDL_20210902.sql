CREATE TABLE UM_S_AF_STDNT_ENRL_BDL_20210902
(
  INSTITUTION_CD      VARCHAR2(5 BYTE),
  ACAD_CAR_CD         VARCHAR2(5 BYTE),
  TERM_CD             VARCHAR2(4 BYTE),
  PERSON_ID           VARCHAR2(11 BYTE),
  SRC_SYS_ID          VARCHAR2(5 BYTE),
  EFFDT_START         DATE,
  EFFDT_END           DATE,
  ACAD_YR             INTEGER,
  AID_YEAR            VARCHAR2(4 BYTE),
  TERM_LD             VARCHAR2(50 BYTE),
  ACAD_ORG_CD         VARCHAR2(5 BYTE),
  ACAD_ORG_LD         VARCHAR2(50 BYTE),
  ACAD_LEVEL_BOT      VARCHAR2(3 BYTE),
  ACAD_PROG_CD        VARCHAR2(5 BYTE),
  ACAD_PROG_LD        VARCHAR2(50 BYTE),
  PROG_CIP_CD         VARCHAR2(13 BYTE),
  ACAD_PLAN_CD        VARCHAR2(10 BYTE),
  ACAD_PLAN_LD        VARCHAR2(50 BYTE),
  PLAN_CIP_CD         VARCHAR2(13 BYTE),
  CE_ONLY_FLG         VARCHAR2(1 BYTE),
  NEW_CONT_IND        VARCHAR2(10 BYTE),
  ONLINE_HYBRID_FLG   VARCHAR2(1 BYTE),
  ONLINE_ONLY_FLG     VARCHAR2(1 BYTE),
  RSDNCY_ID           VARCHAR2(5 BYTE),
  RSDNCY_LD           VARCHAR2(50 BYTE),
  IS_RSDNCY_FLG       VARCHAR2(1 BYTE),
  ONLINE_FTE          NUMBER,
  TOT_FTE             NUMBER,
  ONLINE_CREDITS      NUMBER,
  CE_ONLINE_CREDITS   NUMBER,
  NON_ONLINE_CREDITS  NUMBER,
  CE_CREDITS          NUMBER,
  NON_CE_CREDITS      NUMBER,
  TOT_CREDITS         NUMBER,
  ENROLL_CNT          NUMBER,
  ONLINE_CNT          NUMBER,
  CE_CNT              NUMBER,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE
)
LOGGING 
NOCOMPRESS 
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

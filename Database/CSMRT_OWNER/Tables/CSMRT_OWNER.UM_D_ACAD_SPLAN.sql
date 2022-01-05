CREATE TABLE UM_D_ACAD_SPLAN
(
  ACAD_SPLAN_SID           INTEGER,
  EFFDT                    DATE,
  INSTITUTION_CD           VARCHAR2(5 BYTE),
  ACAD_PLAN_CD             VARCHAR2(10 BYTE),
  ACAD_SPLAN_CD            VARCHAR2(10 BYTE),
  SRC_SYS_ID               VARCHAR2(5 BYTE),
  EFFDT_START              DATE,
  EFFDT_END                DATE,
  EFFDT_ORDER              INTEGER,
  EFF_STAT_CD              VARCHAR2(1 BYTE),
  ACAD_SPLAN_SD            VARCHAR2(10 BYTE),
  ACAD_SPLAN_LD            VARCHAR2(30 BYTE),
  ACAD_SPLAN_CD_DESC       VARCHAR2(50 BYTE),
  ACAD_PLAN_SID            INTEGER,
  INSTITUTION_SID          INTEGER,
  ACAD_SPLAN_TYPE_CD       VARCHAR2(3 BYTE),
  ACAD_SPLAN_TYPE_SD       VARCHAR2(10 BYTE),
  ACAD_SPLAN_TYPE_LD       VARCHAR2(30 BYTE),
  ACAD_SPLAN_TYPE_CD_DESC  VARCHAR2(50 BYTE),
  CIP_CD                   VARCHAR2(13 BYTE),
  CIP_LD                   VARCHAR2(30 BYTE),
  DIPLOMA_LD               VARCHAR2(100 BYTE),
  DIPLOMA_PRINT_FLG        VARCHAR2(1 BYTE),
  EVALUATE_SPLAN_FLG       VARCHAR2(1 BYTE),
  SEV_VALID_CIP_FLG        VARCHAR2(1 BYTE),
  SPLAN_REQTRM_DFLT        VARCHAR2(1 BYTE),
  SPLAN_REQTRM_DFLT_SD     VARCHAR2(10 BYTE),
  SPLAN_REQTRM_DFLT_LD     VARCHAR2(30 BYTE),
  TRNSCR_DESCR             VARCHAR2(100 BYTE),
  TRNSCR_PRINT_FLG         VARCHAR2(1 BYTE),
  UM_STEM_FLG              VARCHAR2(1 BYTE),
  DATA_ORIGIN              VARCHAR2(1 BYTE),
  CREATED_EW_DTTM          DATE,
  LASTUPD_EW_DTTM          DATE
)
NOLOGGING 
NOCOMPRESS 
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

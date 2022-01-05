CREATE TABLE PS_F_EXT_ACAD_SUMM
(
  INSTITUTION_CD          VARCHAR2(5 BYTE),
  PERSON_ID               VARCHAR2(11 BYTE),
  EXT_ORG_ID              VARCHAR2(11 BYTE),
  EXT_ACAD_CAR_ID         VARCHAR2(4 BYTE),
  EXT_DATA_NBR            INTEGER,
  EXT_SUMM_TYPE_ID        VARCHAR2(4 BYTE),
  SRC_SYS_ID              VARCHAR2(5 BYTE),
  INSTITUTION_SID         INTEGER,
  PERSON_SID              INTEGER,
  EXT_ORG_SID             INTEGER,
  EXT_ACAD_CAR_SID        INTEGER,
  EXT_SUMM_TYPE_SID       INTEGER,
  ACAD_RANK_TYPE_SID      INTEGER,
  ACAD_UNIT_TYPE_SID      INTEGER,
  EXT_ACAD_LVL_SID        INTEGER,
  EXT_TERM_SID            INTEGER,
  EXT_TERM_YEAR_SID       INTEGER,
  GPA_TYPE_SID            INTEGER,
  D_EXT_ACAD_LVL_SID      INTEGER,
  D_EXT_TERM_YEAR_SID     INTEGER,
  D_EXT_TERM_SID          INTEGER,
  BEST_SUMM_TYPE_GPA_FLG  VARCHAR2(1 BYTE),
  CLASS_RANK              INTEGER,
  CLASS_SIZE              INTEGER,
  CLASS_PERCENTILE        INTEGER,
  FROM_DT                 DATE,
  TO_DT                   DATE,
  LS_DATA_SOURCE          VARCHAR2(3 BYTE),
  TRNSCR_FLG              VARCHAR2(1 BYTE),
  TRNSCR_TYPE             VARCHAR2(3 BYTE),
  TRNSCR_STATUS           VARCHAR2(1 BYTE),
  TRNSCR_DT               DATE,
  CONVERTED_GPA           NUMBER(6,3),
  EXT_GPA                 NUMBER(6,3),
  UNITS_ATTMPTD           NUMBER(7,2),
  UNITS_CMPLTD            NUMBER(7,2),
  UM_CONVERT_GPA          NUMBER(6,3),
  UM_CUM_CREDIT           NUMBER(5,2),
  UM_CUM_GPA              NUMBER(5,2),
  UM_CUM_QP               NUMBER(5,2),
  UM_EXT_ORG_CR           NUMBER(5,2),
  UM_EXT_ORG_QP           NUMBER(5,2),
  UM_EXT_ORG_GPA          NUMBER(5,2),
  UM_EXT_ORG_CNV_CR       NUMBER(5,2),
  UM_EXT_ORG_CNV_GPA      NUMBER(5,2),
  UM_EXT_ORG_CNV_QP       NUMBER(5,2),
  UM_GPA_EXCLUDE_FLG      VARCHAR2(1 BYTE),
  UM_GPA_OVRD_FLG         VARCHAR2(1 BYTE),
  UM_1_OVRD_HSGPA_FLG     VARCHAR2(1 BYTE),
  UM_EXT_OR_MTSC_GPA      NUMBER(6,3),
  MS_CONVERT_GPA          NUMBER(6,3),
  LOAD_ERROR              VARCHAR2(1 BYTE),
  DATA_ORIGIN             VARCHAR2(1 BYTE),
  CREATED_EW_DTTM         DATE,
  LASTUPD_EW_DTTM         DATE,
  BATCH_SID               INTEGER
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

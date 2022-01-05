CREATE TABLE UM_D_TRANSFER_DICT
(
  TRNSFR_DICT_SID           INTEGER,
  INSTITUTION_CD            VARCHAR2(5 BYTE),
  TRNSFR_SRC_ID             VARCHAR2(11 BYTE),
  COMP_SUBJECT_AREA         VARCHAR2(16 BYTE),
  TRNSFR_EQVLNCY_CMP        VARCHAR2(4 BYTE),
  TRNSFR_CMP_SEQ            INTEGER,
  CRSE_ID                   VARCHAR2(6 BYTE),
  SRC_SYS_ID                VARCHAR2(5 BYTE),
  EFFDT                     DATE,
  EFF_STATUS                VARCHAR2(1 BYTE),
  TRNSFR_SUBJ_DESCR         VARCHAR2(30 BYTE),
  TRNSFR_COMP_DESCR         VARCHAR2(30 BYTE),
  CRSE_SID                  INTEGER,
  EXT_CRSE_SID              INTEGER,
  EXT_ORG_SID               INTEGER,
  EXT_CRSE_OFFER_NBR        INTEGER,
  EXT_TERM_TYPE             VARCHAR2(4 BYTE),
  EXT_TERM_TYPE_SD          VARCHAR2(10 BYTE),
  EXT_TERM_TYPE_LD          VARCHAR2(30 BYTE),
  GRADE_PTS_MIN             NUMBER(9,3),
  GRADE_PTS_MAX             NUMBER(9,3),
  INP_CRSE_CNT              INTEGER,
  INT_TRANSFER_FLG          VARCHAR2(1 BYTE),
  SCHOOL_SUBJECT            VARCHAR2(8 BYTE),
  SCHOOL_CRSE_NBR           VARCHAR2(10 BYTE),
  SSR_MAX_AGE               INTEGER,
  TRNSFR_CRSE_FLG           VARCHAR2(1 BYTE),
  TRNSFR_CRSE_STATUS        VARCHAR2(10 BYTE),
  TRNSFR_GRADE_FLG          VARCHAR2(1 BYTE),
  UM_CRSE_ID                VARCHAR2(6 BYTE),
  UM_CRSE_OFFER_NBR         INTEGER,
  UM_SSR_TR_DEF_GRD_TYP     VARCHAR2(1 BYTE),
  UM_SSR_TR_DEF_GRD_TYP_SD  VARCHAR2(10 BYTE),
  UM_SSR_TR_DEF_GRD_TYP_LD  VARCHAR2(30 BYTE),
  UM_SSR_TR_DEF_GRD_SEQ     VARCHAR2(4 BYTE),
  UM_UNIT_TAKEN             NUMBER(5,2),
  UNITS_MINIMUM             NUMBER(5,2),
  UNITS_MAXIMUM             NUMBER(5,2),
  UNT_TRNSFR_SRC            VARCHAR2(1 BYTE),
  UNT_TRNSFR_SRC_SD         VARCHAR2(10 BYTE),
  UNT_TRNSFR_SRC_LD         VARCHAR2(30 BYTE),
  XS_CRSE_FLG               VARCHAR2(1 BYTE),
  DATA_ORIGIN               VARCHAR2(1 BYTE),
  CREATED_EW_DTTM           DATE,
  LASTUPD_EW_DTTM           DATE
)
NOLOGGING 
COMPRESS BASIC
NO INMEMORY
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

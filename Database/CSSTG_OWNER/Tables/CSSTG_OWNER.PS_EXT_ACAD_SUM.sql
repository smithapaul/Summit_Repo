DROP TABLE CSSTG_OWNER.PS_EXT_ACAD_SUM CASCADE CONSTRAINTS
/

--
-- PS_EXT_ACAD_SUM  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_EXT_ACAD_SUM
(
  EMPLID              VARCHAR2(11 BYTE)         NOT NULL,
  EXT_ORG_ID          VARCHAR2(11 BYTE)         NOT NULL,
  EXT_CAREER          VARCHAR2(4 BYTE)          NOT NULL,
  EXT_DATA_NBR        INTEGER                   NOT NULL,
  EXT_SUMM_TYPE       VARCHAR2(4 BYTE)          NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  EXT_ACAD_LEVEL      VARCHAR2(4 BYTE)          NOT NULL,
  TERM_YEAR           INTEGER                   NOT NULL,
  EXT_TERM_TYPE       VARCHAR2(4 BYTE)          NOT NULL,
  EXT_TERM            VARCHAR2(4 BYTE)          NOT NULL,
  INSTITUTION         VARCHAR2(5 BYTE)          NOT NULL,
  UNT_TYPE            VARCHAR2(3 BYTE)          NOT NULL,
  UNT_ATMP_TOTAL      NUMBER(7,2)               NOT NULL,
  UNT_COMP_TOTAL      NUMBER(7,2)               NOT NULL,
  CLASS_RANK          INTEGER                   NOT NULL,
  CLASS_SIZE          INTEGER                   NOT NULL,
  GPA_TYPE            VARCHAR2(4 BYTE)          NOT NULL,
  EXT_GPA             NUMBER(6,3)               NOT NULL,
  CONVERT_GPA         NUMBER(6,3)               NOT NULL,
  PERCENTILE          INTEGER                   NOT NULL,
  RANK_TYPE           VARCHAR2(3 BYTE)          NOT NULL,
  UM_GPA_EXCLUDE      VARCHAR2(1 BYTE)          NOT NULL,
  UM_EXT_ORG_CR       NUMBER(5,2)               NOT NULL,
  UM_EXT_ORG_QP       NUMBER(5,2)               NOT NULL,
  UM_EXT_ORG_GPA      NUMBER(5,2)               NOT NULL,
  UM_EXT_ORG_CNV_CR   NUMBER(5,2)               NOT NULL,
  UM_EXT_ORG_CNV_GPA  NUMBER(5,2)               NOT NULL,
  UM_EXT_ORG_CNV_QP   NUMBER(5,2)               NOT NULL,
  UM_GPA_OVERRIDE     VARCHAR2(1 BYTE)          NOT NULL,
  UM_1_OVR_HSGPA      VARCHAR2(1 BYTE)          NOT NULL,
  UM_CONVERT_GPA      NUMBER(6,3)               NOT NULL,
  UM_EXT_OR_MTSC_GPA  NUMBER(6,3)               NOT NULL,
  MS_CONVERT_GPA      NUMBER(6,3)               NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

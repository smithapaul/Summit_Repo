DROP TABLE CSSTG_OWNER.PS_TRNS_CRSE_TERM CASCADE CONSTRAINTS
/

--
-- PS_TRNS_CRSE_TERM  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_TRNS_CRSE_TERM
(
  EMPLID              VARCHAR2(11 BYTE)         NOT NULL,
  ACAD_CAREER         VARCHAR2(4 BYTE)          NOT NULL,
  INSTITUTION         VARCHAR2(5 BYTE)          NOT NULL,
  MODEL_NBR           INTEGER                   NOT NULL,
  ARTICULATION_TERM   VARCHAR2(4 BYTE)          NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  MODEL_STATUS        VARCHAR2(1 BYTE)          NOT NULL,
  UNT_TAKEN           NUMBER(5,2)               NOT NULL,
  UNT_TRNSFR          NUMBER(8,3)               NOT NULL,
  TRF_TAKEN_GPA       NUMBER(8,3)               NOT NULL,
  TRF_TAKEN_NOGPA     NUMBER(8,3)               NOT NULL,
  TRF_PASSED_GPA      NUMBER(8,3)               NOT NULL,
  TRF_PASSED_NOGPA    NUMBER(8,3)               NOT NULL,
  TRF_GRADE_POINTS    NUMBER(9,3)               NOT NULL,
  TRF_GPA             NUMBER(8,3)               NOT NULL,
  SSR_FAWI_TKN        NUMBER(5,2)               NOT NULL,
  SSR_FAWI_TKN_GPA    NUMBER(8,3)               NOT NULL,
  SSR_FAWI_TKN_NOGPA  NUMBER(8,3)               NOT NULL,
  SSR_FAWI_PSD        NUMBER(8,3)               NOT NULL,
  SSR_FAWI_PSD_GPA    NUMBER(8,3)               NOT NULL,
  SSR_FAWI_PSD_NOGPA  NUMBER(8,3)               NOT NULL,
  SSR_FAWI_GRADE_PTS  NUMBER(9,3)               NOT NULL,
  SSR_FAWI_GPA        NUMBER(8,3)               NOT NULL,
  POST_DT             DATE,
  OPRID               VARCHAR2(30 BYTE)         NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

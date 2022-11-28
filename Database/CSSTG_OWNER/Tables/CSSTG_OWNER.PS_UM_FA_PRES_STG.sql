DROP TABLE CSSTG_OWNER.PS_UM_FA_PRES_STG CASCADE CONSTRAINTS
/

--
-- PS_UM_FA_PRES_STG  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_UM_FA_PRES_STG
(
  EMPLID            VARCHAR2(11 BYTE)           NOT NULL,
  INSTITUTION       VARCHAR2(5 BYTE)            NOT NULL,
  AID_YEAR          VARCHAR2(4 BYTE)            NOT NULL,
  STRM              VARCHAR2(4 BYTE)            NOT NULL,
  ACAD_CAREER       VARCHAR2(4 BYTE)            NOT NULL,
  SRC_SYS_ID        VARCHAR2(5 BYTE)            NOT NULL,
  FA_LOAD           VARCHAR2(1 BYTE)            NOT NULL,
  DEPNDNCY_STAT     VARCHAR2(1 BYTE)            NOT NULL,
  FISAP_TOT_INC     INTEGER                     NOT NULL,
  FED_NEED          NUMBER(8,2)                 NOT NULL,
  PRIMARY_EFC       INTEGER                     NOT NULL,
  PRORATED_EFC      INTEGER                     NOT NULL,
  FIN_AID_FED_RES   VARCHAR2(5 BYTE)            NOT NULL,
  UM_GRANT_NEED     NUMBER(8,2)                 NOT NULL,
  UM_GRANT_AID      NUMBER(8,2)                 NOT NULL,
  UM_GRANT_DECLINE  NUMBER(8,2)                 NOT NULL,
  UM_GRANT_NN       NUMBER(8,2)                 NOT NULL,
  UM_LOAN_AID       NUMBER(8,2)                 NOT NULL,
  UM_LOAN_NN        NUMBER(8,2)                 NOT NULL,
  UM_LOAN_NEED      NUMBER(8,2)                 NOT NULL,
  UM_LOAN_DECLINE   NUMBER(8,2)                 NOT NULL,
  UM_WORK_NEED      NUMBER(8,2)                 NOT NULL,
  UM_WORK_DECLINE   NUMBER(8,2)                 NOT NULL,
  UM_WORK_NN        NUMBER(8,2)                 NOT NULL,
  UM_WORK_AID       NUMBER(8,2)                 NOT NULL,
  UM_AID_PKG_NEED   NUMBER(11,2)                NOT NULL,
  UM_AID_PKG_NN     NUMBER(11,2)                NOT NULL,
  UM_FALL_ONLY      VARCHAR2(1 BYTE)            NOT NULL,
  UM_SPR_ONLY       VARCHAR2(1 BYTE)            NOT NULL,
  UM_PELL_ONLY      VARCHAR2(1 BYTE)            NOT NULL,
  LOAD_ERROR        VARCHAR2(1 BYTE)            NOT NULL,
  DATA_ORIGIN       VARCHAR2(1 BYTE)            NOT NULL,
  CREATED_EW_DTTM   DATE,
  LASTUPD_EW_DTTM   DATE,
  BATCH_SID         NUMBER(10)                  NOT NULL
)
COMPRESS BASIC
/

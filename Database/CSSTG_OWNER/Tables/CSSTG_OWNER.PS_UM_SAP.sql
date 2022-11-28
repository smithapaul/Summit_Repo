DROP TABLE CSSTG_OWNER.PS_UM_SAP CASCADE CONSTRAINTS
/

--
-- PS_UM_SAP  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_UM_SAP
(
  EMPLID              VARCHAR2(11 BYTE)         NOT NULL,
  INSTITUTION         VARCHAR2(5 BYTE)          NOT NULL,
  AID_YEAR            VARCHAR2(4 BYTE)          NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  ACAD_CAREER         VARCHAR2(4 BYTE)          NOT NULL,
  ACAD_PROG           VARCHAR2(5 BYTE)          NOT NULL,
  RUNDATE             DATE,
  UM_PRIOR_SAP        VARCHAR2(1 BYTE)          NOT NULL,
  UM_START_TERM       VARCHAR2(4 BYTE)          NOT NULL,
  UM_SAP_MIN_GPA      NUMBER(8,3)               NOT NULL,
  UM_SAP_ACT_GPA      NUMBER(8,3)               NOT NULL,
  UM_MAX_TIMEFRAME    INTEGER                   NOT NULL,
  UM_ACT_TIMEFRAME    INTEGER                   NOT NULL,
  UM_SAP_PART_SEM     INTEGER                   NOT NULL,
  UM_SAP_FULL_SEM     INTEGER                   NOT NULL,
  UM_MAX_CREDITS      NUMBER(8,3)               NOT NULL,
  UM_MIN_COMPLETE_RT  NUMBER(5,2)               NOT NULL,
  UM_ACT_COMPLETE_RT  NUMBER(5,2)               NOT NULL,
  UM_TOT_ATT_CRED     NUMBER(8,3)               NOT NULL,
  UM_UMASS_ATT_CRED   NUMBER(8,3)               NOT NULL,
  UM_TRN_ATT_CRED     NUMBER(8,3)               NOT NULL,
  UM_TOT_ERN_CRED     NUMBER(8,3)               NOT NULL,
  UM_UMASS_ERN_CRED   NUMBER(8,3)               NOT NULL,
  UM_TRN_ERN_CRED     NUMBER(8,3)               NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

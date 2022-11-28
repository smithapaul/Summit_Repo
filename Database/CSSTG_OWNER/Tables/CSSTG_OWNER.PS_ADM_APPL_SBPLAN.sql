DROP TABLE CSSTG_OWNER.PS_ADM_APPL_SBPLAN CASCADE CONSTRAINTS
/

--
-- PS_ADM_APPL_SBPLAN  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_ADM_APPL_SBPLAN
(
  EMPLID           VARCHAR2(11 BYTE)            NOT NULL,
  ACAD_CAREER      VARCHAR2(4 BYTE)             NOT NULL,
  STDNT_CAR_NBR    INTEGER                      NOT NULL,
  ADM_APPL_NBR     VARCHAR2(8 BYTE)             NOT NULL,
  APPL_PROG_NBR    INTEGER                      NOT NULL,
  EFFDT            DATE                         NOT NULL,
  EFFSEQ           INTEGER                      NOT NULL,
  ACAD_PLAN        VARCHAR2(10 BYTE)            NOT NULL,
  ACAD_SUB_PLAN    VARCHAR2(10 BYTE)            NOT NULL,
  SRC_SYS_ID       VARCHAR2(5 BYTE)             NOT NULL,
  DECLARE_DT       DATE                         NOT NULL,
  REQ_TERM         VARCHAR2(4 BYTE)             NOT NULL,
  LOAD_ERROR       VARCHAR2(1 BYTE)             NOT NULL,
  DATA_ORIGIN      VARCHAR2(1 BYTE)             NOT NULL,
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE,
  BATCH_SID        NUMBER(10)                   NOT NULL
)
COMPRESS BASIC
/

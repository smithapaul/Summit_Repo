DROP TABLE CSSTG_OWNER.PS_STND_MTGPAT_TBL CASCADE CONSTRAINTS
/

--
-- PS_STND_MTGPAT_TBL  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_STND_MTGPAT_TBL
(
  INSTITUTION      VARCHAR2(5 BYTE)             NOT NULL,
  ACAD_GROUP       VARCHAR2(5 BYTE)             NOT NULL,
  EFFDT            DATE                         NOT NULL,
  STND_MTG_PAT     VARCHAR2(4 BYTE)             NOT NULL,
  SRC_SYS_ID       VARCHAR2(5 BYTE)             NOT NULL,
  DESCR            VARCHAR2(30 BYTE)            NOT NULL,
  DESCRSHORT       VARCHAR2(10 BYTE)            NOT NULL,
  MON              VARCHAR2(1 BYTE)             NOT NULL,
  TUES             VARCHAR2(1 BYTE)             NOT NULL,
  WED              VARCHAR2(1 BYTE)             NOT NULL,
  THURS            VARCHAR2(1 BYTE)             NOT NULL,
  FRI              VARCHAR2(1 BYTE)             NOT NULL,
  SAT              VARCHAR2(1 BYTE)             NOT NULL,
  SUN              VARCHAR2(1 BYTE)             NOT NULL,
  NORM_DURATION    INTEGER                      NOT NULL,
  LOAD_ERROR       VARCHAR2(1 BYTE)             NOT NULL,
  DATA_ORIGIN      VARCHAR2(1 BYTE)             NOT NULL,
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE,
  BATCH_SID        NUMBER(10)                   NOT NULL
)
COMPRESS BASIC
/

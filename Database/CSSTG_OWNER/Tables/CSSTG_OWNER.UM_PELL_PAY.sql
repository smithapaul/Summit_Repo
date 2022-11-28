DROP TABLE CSSTG_OWNER.UM_PELL_PAY CASCADE CONSTRAINTS
/

--
-- UM_PELL_PAY  (Table) 
--
CREATE TABLE CSSTG_OWNER.UM_PELL_PAY
(
  AID_YEAR         VARCHAR2(4 BYTE)             NOT NULL,
  FA_LOAD          VARCHAR2(1 BYTE)             NOT NULL,
  COA_MIN          INTEGER,
  COA_MAX          INTEGER,
  EFC_MIN          INTEGER,
  EFC_MAX          INTEGER,
  PELL_AMT         INTEGER,
  LOAD_ERROR       VARCHAR2(1 BYTE)             DEFAULT 'N',
  DATA_ORIGIN      VARCHAR2(1 BYTE)             DEFAULT 'S',
  CREATED_EW_DTTM  DATE                         DEFAULT sysdate,
  LASTUPD_EW_DTTM  DATE                         DEFAULT sysdate,
  BATCH_SID        NUMBER(10)                   DEFAULT 1234
)
COMPRESS BASIC
/

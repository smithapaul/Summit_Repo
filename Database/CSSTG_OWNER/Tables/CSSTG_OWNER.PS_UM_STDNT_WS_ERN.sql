DROP TABLE CSSTG_OWNER.PS_UM_STDNT_WS_ERN CASCADE CONSTRAINTS
/

--
-- PS_UM_STDNT_WS_ERN  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_UM_STDNT_WS_ERN
(
  EMPLID           VARCHAR2(11 BYTE)            NOT NULL,
  INSTITUTION      VARCHAR2(5 BYTE)             NOT NULL,
  AID_YEAR         VARCHAR2(4 BYTE)             NOT NULL,
  PAY_END_DT       DATE                         NOT NULL,
  EMPL_RCD         INTEGER                      NOT NULL,
  ACCT_CD          VARCHAR2(25 BYTE)            NOT NULL,
  DEPTID           VARCHAR2(10 BYTE)            NOT NULL,
  ITEM_TYPE        VARCHAR2(12 BYTE)            NOT NULL,
  LAST_RUN_DT      DATE                         NOT NULL,
  ERN_BEGIN_DT     DATE                         NOT NULL,
  ERN_END_DT       DATE                         NOT NULL,
  SRC_SYS_ID       VARCHAR2(5 BYTE)             NOT NULL,
  HOURLY_RT        NUMBER(18,6),
  RATE_USED        VARCHAR2(1 BYTE),
  REG_HRS          NUMBER(6,2),
  REG_EARNS        NUMBER(10,2),
  UM_REG_EARNS     NUMBER(14,6),
  LOAD_ERROR       VARCHAR2(1 BYTE)             NOT NULL,
  DATA_ORIGIN      VARCHAR2(1 BYTE)             NOT NULL,
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE,
  BATCH_SID        NUMBER(10)                   NOT NULL
)
COMPRESS BASIC
/

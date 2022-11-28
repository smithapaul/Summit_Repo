DROP TABLE CSSTG_OWNER.PS_S_RT_RATE_TBL_OLD CASCADE CONSTRAINTS
/

--
-- PS_S_RT_RATE_TBL_OLD  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_S_RT_RATE_TBL_OLD
(
  RT_RATE_INDEX    VARCHAR2(10 BYTE)            NOT NULL,
  TERM             INTEGER                      NOT NULL,
  FROM_CUR         VARCHAR2(3 BYTE)             NOT NULL,
  TO_CUR           VARCHAR2(3 BYTE)             NOT NULL,
  RT_TYPE          VARCHAR2(5 BYTE)             NOT NULL,
  EFFDT            DATE,
  SRC_SYS_ID       VARCHAR2(5 BYTE)             NOT NULL,
  RATE_MULT        NUMBER(15,8)                 NOT NULL,
  RATE_DIV         NUMBER(15,8)                 NOT NULL,
  SYNCID           INTEGER                      NOT NULL,
  LASTUPDDTTM      DATE,
  TIMEZONE         VARCHAR2(9 BYTE)             NOT NULL,
  LOAD_ERROR       VARCHAR2(1 BYTE)             NOT NULL,
  DATA_ORIGIN      VARCHAR2(1 BYTE)             NOT NULL,
  CREATED_EW_DTTM  DATE,
  LASTUPD_EW_DTTM  DATE,
  BATCH_SID        NUMBER(10)                   NOT NULL
)
COMPRESS BASIC
/

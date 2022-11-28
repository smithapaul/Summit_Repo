DROP TABLE CSSTG_OWNER.PS_ITEM_TYPE_FISCL CASCADE CONSTRAINTS
/

--
-- PS_ITEM_TYPE_FISCL  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_ITEM_TYPE_FISCL
(
  SETID               VARCHAR2(5 BYTE)          NOT NULL,
  ITEM_TYPE           VARCHAR2(12 BYTE)         NOT NULL,
  AID_YEAR            VARCHAR2(4 BYTE)          NOT NULL,
  SRC_SYS_ID          VARCHAR2(5 BYTE)          NOT NULL,
  INSTITUTION         VARCHAR2(5 BYTE)          NOT NULL,
  MAX_OFR_BUDGT       NUMBER(18,3)              NOT NULL,
  OFR_GROSS           NUMBER(18,3)              NOT NULL,
  OFR_REDUCTN         NUMBER(18,3)              NOT NULL,
  OFR_NET             NUMBER(18,3)              NOT NULL,
  OFR_AVAILABLE       NUMBER(18,3)              NOT NULL,
  OFFERED_NET_COUNT   INTEGER                   NOT NULL,
  MAX_ACC_BUDGT       NUMBER(18,3)              NOT NULL,
  ACC_GROSS           NUMBER(18,3)              NOT NULL,
  ACC_REDUCTN         NUMBER(18,3)              NOT NULL,
  ACC_NET             NUMBER(18,3)              NOT NULL,
  ACC_AVAILABLE       NUMBER(18,3)              NOT NULL,
  ACCEPTED_NET_COUNT  INTEGER                   NOT NULL,
  DECLINED_AMTS       NUMBER(18,3)              NOT NULL,
  DECLINED_COUNT      INTEGER                   NOT NULL,
  CANCELLED_AMTS      NUMBER(18,3)              NOT NULL,
  CANCELLED_COUNT     INTEGER                   NOT NULL,
  MAX_ATH_BUDGT       NUMBER(18,3)              NOT NULL,
  NET_ATH_AMT         NUMBER(18,3)              NOT NULL,
  MAX_DSB_BUDGT       NUMBER(18,3)              NOT NULL,
  NET_DSB_PAID        NUMBER(18,3)              NOT NULL,
  POTENTIAL_DSB       NUMBER(18,3)              NOT NULL,
  CURRENCY_CD         VARCHAR2(3 BYTE)          NOT NULL,
  HIGHEST_ACC_SUM     NUMBER(18,3)              NOT NULL,
  HIGHEST_OFR_SUM     NUMBER(18,3)              NOT NULL,
  LOAD_ERROR          VARCHAR2(1 BYTE)          NOT NULL,
  DATA_ORIGIN         VARCHAR2(1 BYTE)          NOT NULL,
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10)                NOT NULL
)
COMPRESS BASIC
/

CREATE TABLE PS_S_SF_ACCTG_LN
(
  UM_FISCAL_YEAR      INTEGER,
  RUN_DT              DATE,
  SEQNUM              INTEGER,
  SF_LINE_NBR         INTEGER,
  SRC_SYS_ID          VARCHAR2(5 BYTE),
  IN_PROCESS_FLG      VARCHAR2(1 BYTE),
  BUSINESS_UNIT_GL    VARCHAR2(5 BYTE),
  JOURNAL_ID          VARCHAR2(10 BYTE),
  JOURNAL_DATE        DATE,
  JOURNAL_LINE        INTEGER,
  ACCOUNT             VARCHAR2(10 BYTE),
  FUND_CODE           VARCHAR2(5 BYTE),
  PROGRAM_CODE        VARCHAR2(5 BYTE),
  DEPTID              VARCHAR2(10 BYTE),
  PROJECT_ID          VARCHAR2(15 BYTE),
  STATISTICS_CODE     VARCHAR2(3 BYTE),
  MONETARY_AMOUNT     NUMBER(15,2),
  STATISTIC_AMOUNT    NUMBER(15,2),
  JRNL_LN_REF         VARCHAR2(10 BYTE),
  OPEN_ITEM_STATUS    VARCHAR2(1 BYTE),
  LINE_DESCR          VARCHAR2(30 BYTE),
  JRNL_LINE_STATUS    VARCHAR2(1 BYTE),
  JOURNAL_LINE_DATE   DATE,
  BUSINESS_UNIT       VARCHAR2(5 BYTE),
  APPL_JRNL_ID        VARCHAR2(10 BYTE),
  ACCOUNTING_DT       DATE,
  GL_DISTRIB_STATUS   VARCHAR2(1 BYTE),
  PROCESS_INSTANCE    NUMBER(10),
  CURRENCY_CD         VARCHAR2(3 BYTE),
  ACCOUNTING_PERIOD   INTEGER,
  FISCAL_YEAR         INTEGER,
  FOREIGN_AMOUNT      NUMBER(15,2),
  FOREIGN_CURRENCY    VARCHAR2(3 BYTE),
  LEDGER              VARCHAR2(10 BYTE),
  LEDGER_GROUP        VARCHAR2(10 BYTE),
  EXT_GL_CHARTFLD     VARCHAR2(50 BYTE),
  EMPLID              VARCHAR2(11 BYTE),
  SF_EXT_ORG_ID       VARCHAR2(11 BYTE),
  ITEM_NBR            VARCHAR2(15 BYTE),
  BUDGET_PERIOD       VARCHAR2(10 BYTE),
  CLASS_FLD           VARCHAR2(5 BYTE),
  AFFILIATE           VARCHAR2(5 BYTE),
  BUDGET_REF          VARCHAR2(8 BYTE),
  CHARTFIELD1         VARCHAR2(10 BYTE),
  CHARTFIELD2         VARCHAR2(10 BYTE),
  CHARTFIELD3         VARCHAR2(10 BYTE),
  ALTACCT             VARCHAR2(10 BYTE),
  OPERATING_UNIT      VARCHAR2(8 BYTE),
  PRODUCT             VARCHAR2(6 BYTE),
  AFFILIATE_INTRA1    VARCHAR2(10 BYTE),
  AFFILIATE_INTRA2    VARCHAR2(10 BYTE),
  SF_DEPOSIT_ID       VARCHAR2(10 BYTE),
  RT_TYPE             VARCHAR2(5 BYTE),
  RATE_DIV            NUMBER(15,8),
  RATE_MULT           NUMBER(15,8),
  SF_GL_RUN_INSTANCE  NUMBER(10),
  AUDIT_ACTN          VARCHAR2(1 BYTE),
  COMMON_ID           VARCHAR2(11 BYTE),
  SA_ID_TYPE          VARCHAR2(1 BYTE),
  SSF_GL_TRANS_ID     VARCHAR2(12 BYTE),
  SSF_GL_TRANS_SEQNO  INTEGER,
  SCC_ROW_ADD_OPRID   VARCHAR2(30 BYTE),
  SCC_ROW_ADD_DTTM    DATE,
  SCC_ROW_UPD_OPRID   VARCHAR2(30 BYTE),
  SCC_ROW_UPD_DTTM    DATE,
  ITEM_TERM           VARCHAR2(4 BYTE),
  ITEM_TYPE           VARCHAR2(12 BYTE),
  REF1_DESCR          VARCHAR2(30 BYTE),
  RECEIPT_NBR         NUMBER(12),
  LOAD_ERROR          VARCHAR2(1 BYTE),
  DATA_ORIGIN         VARCHAR2(1 BYTE),
  CREATED_EW_DTTM     DATE,
  LASTUPD_EW_DTTM     DATE,
  BATCH_SID           NUMBER(10),
  UM_SNAPSHOT_DT      DATE
)
NOCOMPRESS 
NO INMEMORY
PARTITION BY LIST (UM_FISCAL_YEAR)
(  
  PARTITION FY_2021 VALUES (2021)
    READ WRITE
    NO INMEMORY
    NOLOGGING
    NOCOMPRESS
)
NOCACHE
RESULT_CACHE (MODE DEFAULT)
NOPARALLEL;

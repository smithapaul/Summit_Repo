DROP TABLE CSSTG_OWNER.PS_K_SF_ACCTG_LN CASCADE CONSTRAINTS
/

--
-- PS_K_SF_ACCTG_LN  (Table) 
--
CREATE TABLE CSSTG_OWNER.PS_K_SF_ACCTG_LN
(
  RUN_DT       DATE                             NOT NULL,
  SEQNUM       INTEGER                          NOT NULL,
  SF_LINE_NBR  INTEGER                          NOT NULL,
  SRC_SYS_ID   VARCHAR2(5 BYTE)                 NOT NULL
)
NOCOMPRESS
/

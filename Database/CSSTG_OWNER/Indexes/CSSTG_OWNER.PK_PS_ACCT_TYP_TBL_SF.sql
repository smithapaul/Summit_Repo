DROP INDEX CSSTG_OWNER.PK_PS_ACCT_TYP_TBL_SF
/

--
-- PK_PS_ACCT_TYP_TBL_SF  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACCT_TYP_TBL_SF ON CSSTG_OWNER.PS_ACCT_TYP_TBL_SF
(SETID, ACCOUNT_TYPE_SF, SRC_SYS_ID)
/

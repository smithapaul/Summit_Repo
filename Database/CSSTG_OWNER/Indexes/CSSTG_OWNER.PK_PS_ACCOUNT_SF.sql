DROP INDEX CSSTG_OWNER.PK_PS_ACCOUNT_SF
/

--
-- PK_PS_ACCOUNT_SF  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACCOUNT_SF ON CSSTG_OWNER.PS_ACCOUNT_SF
(BUSINESS_UNIT, EMPLID, ACCOUNT_NBR, ACCOUNT_TERM, SRC_SYS_ID)
/

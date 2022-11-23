DROP INDEX CSMRT_OWNER.PK_UM_F_SF_BILLING
/

--
-- PK_UM_F_SF_BILLING  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_F_SF_BILLING ON CSMRT_OWNER.UM_F_SF_BILLING
(INSTITUTION_CD, INVOICE_ID, ITEM_NBR, ITEM_LINE, SRC_SYS_ID)
/

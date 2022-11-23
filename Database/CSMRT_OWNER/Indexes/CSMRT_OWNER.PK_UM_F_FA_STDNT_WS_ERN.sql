DROP INDEX CSMRT_OWNER.PK_UM_F_FA_STDNT_WS_ERN
/

--
-- PK_UM_F_FA_STDNT_WS_ERN  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_F_FA_STDNT_WS_ERN ON CSMRT_OWNER.UM_F_FA_STDNT_WS_ERN
(EMPLID, INSTITUTION, AID_YEAR, PAY_END_DT, EMPL_RCD, 
ACCT_CD, DEPTID, ITEM_TYPE, LAST_RUN_DT, ERN_BEGIN_DT, 
ERN_END_DT, SRC_SYS_ID)
/

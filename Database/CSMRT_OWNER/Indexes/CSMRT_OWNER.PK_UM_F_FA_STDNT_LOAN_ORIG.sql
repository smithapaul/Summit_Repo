DROP INDEX CSMRT_OWNER.PK_UM_F_FA_STDNT_LOAN_ORIG
/

--
-- PK_UM_F_FA_STDNT_LOAN_ORIG  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_F_FA_STDNT_LOAN_ORIG ON CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG
(INSTITUTION_CD, ACAD_CAR_CD, AID_YEAR, PERSON_ID, LOAN_TYPE, 
LN_APPL_SEQ, ITEM_TYPE, SRC_SYS_ID)
/

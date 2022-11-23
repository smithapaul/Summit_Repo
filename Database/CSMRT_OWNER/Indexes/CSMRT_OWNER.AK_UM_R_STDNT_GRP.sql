DROP INDEX CSMRT_OWNER.AK_UM_R_STDNT_GRP
/

--
-- AK_UM_R_STDNT_GRP  (Index) 
--
CREATE INDEX CSMRT_OWNER.AK_UM_R_STDNT_GRP ON CSMRT_OWNER.UM_R_STDNT_GRP
(INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, STDNT_GRP_CD)
/

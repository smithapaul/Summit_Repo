DROP INDEX CSMRT_OWNER.PK_PS_F_EXT_ACAD_SUMM
/

--
-- PK_PS_F_EXT_ACAD_SUMM  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_PS_F_EXT_ACAD_SUMM ON CSMRT_OWNER.PS_F_EXT_ACAD_SUMM
(INSTITUTION_CD, PERSON_ID, EXT_ORG_ID, EXT_ACAD_CAR_ID, EXT_DATA_NBR, 
EXT_SUMM_TYPE_ID, SRC_SYS_ID)
/

DROP INDEX CSSTG_OWNER.PK_PS_UM_HS_GPA_CALC
/

--
-- PK_PS_UM_HS_GPA_CALC  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_UM_HS_GPA_CALC ON CSSTG_OWNER.PS_UM_HS_GPA_CALC
(EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUMM_TYPE, 
INSTITUTION, EXT_COURSE_NBR, SRC_SYS_ID)
/

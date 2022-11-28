DROP INDEX CSSTG_OWNER.PK_PS_UM_CUMGPA
/

--
-- PK_PS_UM_CUMGPA  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_UM_CUMGPA ON CSSTG_OWNER.PS_UM_CUMGPA
(EMPLID, INSTITUTION, EXT_SUMM_TYPE, SRC_SYS_ID)
/

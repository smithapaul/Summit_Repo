DROP INDEX CSMRT_OWNER.PK_UM_W_RA_EXTRACT_CONTROL
/

--
-- PK_UM_W_RA_EXTRACT_CONTROL  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_W_RA_EXTRACT_CONTROL ON CSMRT_OWNER.UM_W_RA_EXTRACT_CONTROL
(INSTITUTION_CD, EXTRACT_TYPE)
/

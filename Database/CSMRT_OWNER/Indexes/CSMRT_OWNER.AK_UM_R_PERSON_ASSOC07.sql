DROP INDEX CSMRT_OWNER.AK_UM_R_PERSON_ASSOC07
/

--
-- AK_UM_R_PERSON_ASSOC07  (Index) 
--
CREATE BITMAP INDEX CSMRT_OWNER.AK_UM_R_PERSON_ASSOC07 ON CSMRT_OWNER.UM_R_PERSON_ASSOC
(ENRLMT_MIN_PERSON_TERM_SID)
/

DROP INDEX CSMRT_OWNER.AK_F_ADM_APPL_CLS_ENRL_INST
/

--
-- AK_F_ADM_APPL_CLS_ENRL_INST  (Index) 
--
CREATE BITMAP INDEX CSMRT_OWNER.AK_F_ADM_APPL_CLS_ENRL_INST ON CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT
(INSTITUTION_SID)
/

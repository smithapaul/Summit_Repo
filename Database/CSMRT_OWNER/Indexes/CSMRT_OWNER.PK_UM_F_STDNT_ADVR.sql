DROP INDEX CSMRT_OWNER.PK_UM_F_STDNT_ADVR
/

--
-- PK_UM_F_STDNT_ADVR  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_F_STDNT_ADVR ON CSMRT_OWNER.UM_F_STDNT_ADVR
(TERM_SID, PERSON_SID, STDNT_CAR_NUM, ACAD_PLAN_SID, ACAD_SPLAN_SID, 
ADVISOR_ROLE, STDNT_ADVISOR_NBR, SRC_SYS_ID)
/

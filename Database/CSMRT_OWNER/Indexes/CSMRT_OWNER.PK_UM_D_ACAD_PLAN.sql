DROP INDEX CSMRT_OWNER.PK_UM_D_ACAD_PLAN
/

--
-- PK_UM_D_ACAD_PLAN  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.PK_UM_D_ACAD_PLAN ON CSMRT_OWNER.UM_D_ACAD_PLAN
(ACAD_PLAN_SID, EFFDT)
/

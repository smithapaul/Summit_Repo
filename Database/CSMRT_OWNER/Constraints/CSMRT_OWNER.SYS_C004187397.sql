ALTER TABLE CSMRT_OWNER.UM_D_ACAD_PLAN MODIFY 
  ACAD_PLAN_CD_DESC NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_ACAD_PLAN MODIFY 
  ACAD_PLAN_CD_DESC NOT NULL
  ENABLE VALIDATE
/

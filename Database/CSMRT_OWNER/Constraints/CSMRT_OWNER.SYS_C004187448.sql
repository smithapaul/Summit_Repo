ALTER TABLE CSMRT_OWNER.UM_D_ACAD_PLAN MODIFY 
  SSR_NSC_INCL_PLAN_FLG NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_ACAD_PLAN MODIFY 
  SSR_NSC_INCL_PLAN_FLG NOT NULL
  ENABLE VALIDATE
/

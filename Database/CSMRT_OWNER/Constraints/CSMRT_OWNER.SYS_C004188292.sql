ALTER TABLE CSMRT_OWNER.UM_D_CLASS_ASSOC MODIFY 
  GRADE_ROSTER_PRINT_SD NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_CLASS_ASSOC MODIFY 
  GRADE_ROSTER_PRINT_SD NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSMRT_OWNER.UM_D_EMPLOYEE_HIST
  DROP CONSTRAINT PK_UM_D_EMPLOYEE_HIST
/

ALTER TABLE CSMRT_OWNER.UM_D_EMPLOYEE_HIST ADD (
  CONSTRAINT PK_UM_D_EMPLOYEE_HIST
  PRIMARY KEY
  (PERSON_ID, EMPL_RCD, EFF_START_DT, EFFSEQ, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/

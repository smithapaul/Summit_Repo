ALTER TABLE CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR MODIFY 
  GRADE_ROSTER_STAT_SD NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR MODIFY 
  GRADE_ROSTER_STAT_SD NOT NULL
  ENABLE VALIDATE
/

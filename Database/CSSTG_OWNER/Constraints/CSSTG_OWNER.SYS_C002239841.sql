ALTER TABLE CSSTG_OWNER.PS_GRADE_RSTR_TYPE MODIFY 
  OVRD_GRADE_ROSTER NULL
/

ALTER TABLE CSSTG_OWNER.PS_GRADE_RSTR_TYPE MODIFY 
  OVRD_GRADE_ROSTER NOT NULL
  ENABLE VALIDATE
/

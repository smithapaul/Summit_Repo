ALTER TABLE CSSTG_OWNER.PS_T_GRADE_ROSTER MODIFY 
  GRADING_BASIS_ENRL NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_GRADE_ROSTER MODIFY 
  GRADING_BASIS_ENRL NOT NULL
  ENABLE VALIDATE
/

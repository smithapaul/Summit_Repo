ALTER TABLE CSSTG_OWNER.PS_AUD_GRADE MODIFY 
  OVRD_GRADING_BASIS NULL
/

ALTER TABLE CSSTG_OWNER.PS_AUD_GRADE MODIFY 
  OVRD_GRADING_BASIS NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSSTG_OWNER.PS_PELL_DISBMNT MODIFY 
  PELL_ORIG_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_PELL_DISBMNT MODIFY 
  PELL_ORIG_ID NOT NULL
  ENABLE VALIDATE
/

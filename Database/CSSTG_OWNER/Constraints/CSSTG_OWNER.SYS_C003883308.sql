ALTER TABLE CSSTG_OWNER.PS_UM_STD_LST_ATND MODIFY 
  UM_STD_NEVER_ATTND NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_STD_LST_ATND MODIFY 
  UM_STD_NEVER_ATTND NOT NULL
  ENABLE VALIDATE
/

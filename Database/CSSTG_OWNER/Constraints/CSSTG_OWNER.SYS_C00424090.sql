ALTER TABLE CSSTG_OWNER.PS_PERS_APPL_INFO_OLD MODIFY 
  SRC_SYS_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_PERS_APPL_INFO_OLD MODIFY 
  SRC_SYS_ID NOT NULL
  ENABLE VALIDATE
/

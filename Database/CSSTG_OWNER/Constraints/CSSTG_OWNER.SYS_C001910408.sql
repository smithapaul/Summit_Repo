ALTER TABLE CSSTG_OWNER.PS_UM_EMPLOYEES MODIFY 
  UM_WOR_EXT_HRS_BAL NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_EMPLOYEES MODIFY 
  UM_WOR_EXT_HRS_BAL NOT NULL
  ENABLE VALIDATE
/

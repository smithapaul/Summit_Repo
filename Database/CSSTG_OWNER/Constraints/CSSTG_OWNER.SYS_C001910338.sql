ALTER TABLE CSSTG_OWNER.PS_UM_EMPLOYEES MODIFY 
  UM_POSN_DESCR_TTL NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_EMPLOYEES MODIFY 
  UM_POSN_DESCR_TTL NOT NULL
  ENABLE VALIDATE
/

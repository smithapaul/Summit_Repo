ALTER TABLE CSMRT_OWNER.UM_D_CLASS MODIFY 
  WAITLIST_DAEMON NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_CLASS MODIFY 
  WAITLIST_DAEMON NOT NULL
  ENABLE VALIDATE
/

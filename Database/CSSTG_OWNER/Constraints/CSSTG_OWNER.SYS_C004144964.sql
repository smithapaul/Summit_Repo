ALTER TABLE CSSTG_OWNER.PS_EVENT_MTG MODIFY 
  COORDINATOR_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_EVENT_MTG MODIFY 
  COORDINATOR_ID NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSMRT_OWNER.UM_D_CRSE_TOPIC
  DROP CONSTRAINT PK_UM_D_CRSE_TOPIC
/

ALTER TABLE CSMRT_OWNER.UM_D_CRSE_TOPIC ADD (
  CONSTRAINT PK_UM_D_CRSE_TOPIC
  PRIMARY KEY
  (CRSE_TOPIC_SID)
  RELY
  ENABLE VALIDATE)
/

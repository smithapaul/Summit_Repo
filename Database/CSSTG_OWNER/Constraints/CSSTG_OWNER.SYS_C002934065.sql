ALTER TABLE CSSTG_OWNER.PS_COMMUNICATION MODIFY 
  COMM_DTTM NULL
/

ALTER TABLE CSSTG_OWNER.PS_COMMUNICATION MODIFY 
  COMM_DTTM NOT NULL
  ENABLE VALIDATE
/

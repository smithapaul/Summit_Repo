ALTER TABLE CSSTG_OWNER.PS_COMMUNICATION MODIFY 
  EMPLID_RELATED NULL
/

ALTER TABLE CSSTG_OWNER.PS_COMMUNICATION MODIFY 
  EMPLID_RELATED NOT NULL
  ENABLE VALIDATE
/

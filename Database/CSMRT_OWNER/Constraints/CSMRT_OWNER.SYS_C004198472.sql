ALTER TABLE CSMRT_OWNER.UM_R_PERSON_ASSOC MODIFY 
  PERSON_SRVC_IND_SID NULL
/

ALTER TABLE CSMRT_OWNER.UM_R_PERSON_ASSOC MODIFY 
  PERSON_SRVC_IND_SID NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSMRT_OWNER.UM_R_PERSON_ASSOC MODIFY 
  BEACON_CARD_ID NULL
/

ALTER TABLE CSMRT_OWNER.UM_R_PERSON_ASSOC MODIFY 
  BEACON_CARD_ID NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM_LINE MODIFY 
  PERSON_ID NULL
/

ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM_LINE MODIFY 
  PERSON_ID NOT NULL
  ENABLE VALIDATE
/

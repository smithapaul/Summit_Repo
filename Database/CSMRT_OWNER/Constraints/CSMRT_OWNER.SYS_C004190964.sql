ALTER TABLE CSMRT_OWNER.PS_R_PERSON_RSDNCY MODIFY 
  FA_FED_RSDNCY_ID NULL
/

ALTER TABLE CSMRT_OWNER.PS_R_PERSON_RSDNCY MODIFY 
  FA_FED_RSDNCY_ID NOT NULL
  ENABLE VALIDATE
/

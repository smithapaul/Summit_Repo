ALTER TABLE CSMRT_OWNER.UM_R_PERSON_RSDNCY MODIFY 
  TUITION_RSDNCY_EXCPTN NULL
/

ALTER TABLE CSMRT_OWNER.UM_R_PERSON_RSDNCY MODIFY 
  TUITION_RSDNCY_EXCPTN NOT NULL
  ENABLE VALIDATE
/

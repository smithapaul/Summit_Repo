ALTER TABLE CSMRT_OWNER.UM_S_SR_DATA MODIFY 
  INSTITUTION_CD NULL
/

ALTER TABLE CSMRT_OWNER.UM_S_SR_DATA MODIFY 
  INSTITUTION_CD NOT NULL
  ENABLE VALIDATE
/

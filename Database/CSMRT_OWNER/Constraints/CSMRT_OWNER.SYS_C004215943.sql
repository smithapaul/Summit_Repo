ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM MODIFY 
  INSTITUTION_CD NULL
/

ALTER TABLE CSMRT_OWNER.UM_S_SF_ITEM MODIFY 
  INSTITUTION_CD NOT NULL
  ENABLE VALIDATE
/

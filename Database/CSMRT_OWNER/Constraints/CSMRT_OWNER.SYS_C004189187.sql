ALTER TABLE CSMRT_OWNER.PS_D_EXT_ORG MODIFY 
  EXT_ORG_TYPE_SD NULL
/

ALTER TABLE CSMRT_OWNER.PS_D_EXT_ORG MODIFY 
  EXT_ORG_TYPE_SD NOT NULL
  ENABLE VALIDATE
/

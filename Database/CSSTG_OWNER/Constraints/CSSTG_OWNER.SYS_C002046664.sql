ALTER TABLE CSSTG_OWNER.PS_CIP_CODE_TBL MODIFY 
  CIP_ALTERNATIVE_CD NULL
/

ALTER TABLE CSSTG_OWNER.PS_CIP_CODE_TBL MODIFY 
  CIP_ALTERNATIVE_CD NOT NULL
  ENABLE VALIDATE
/

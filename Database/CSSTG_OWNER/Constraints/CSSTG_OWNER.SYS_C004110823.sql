ALTER TABLE CSSTG_OWNER.PS_AUDIT_SRVC_IND MODIFY 
  EXT_ORG_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_AUDIT_SRVC_IND MODIFY 
  EXT_ORG_ID NOT NULL
  ENABLE VALIDATE
/

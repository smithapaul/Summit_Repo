ALTER TABLE CSSTG_OWNER.PS_AUDIT_SRVC_IND MODIFY 
  AUDIT_OPRID NULL
/

ALTER TABLE CSSTG_OWNER.PS_AUDIT_SRVC_IND MODIFY 
  AUDIT_OPRID NOT NULL
  ENABLE VALIDATE
/

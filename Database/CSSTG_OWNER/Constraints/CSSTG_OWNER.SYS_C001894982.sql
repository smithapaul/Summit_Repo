ALTER TABLE CSSTG_OWNER.PS_AUDIT_CLSMTG_UM MODIFY 
  PRINT_TOPIC_ON_XCR NULL
/

ALTER TABLE CSSTG_OWNER.PS_AUDIT_CLSMTG_UM MODIFY 
  PRINT_TOPIC_ON_XCR NOT NULL
  ENABLE VALIDATE
/

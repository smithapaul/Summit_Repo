ALTER TABLE CSSTG_OWNER.PS_EXT_COURSE MODIFY 
  EXT_TERM_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_EXT_COURSE MODIFY 
  EXT_TERM_TYPE NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSSTG_OWNER.UM_FA_IR_CDS_STG
  DROP CONSTRAINT PK_UM_FA_IR_CDS_STG
/

ALTER TABLE CSSTG_OWNER.UM_FA_IR_CDS_STG ADD (
  CONSTRAINT PK_UM_FA_IR_CDS_STG
  PRIMARY KEY
  (INSTITUTION, TERM, EMPLID, CAREER)
  RELY
  ENABLE VALIDATE)
/

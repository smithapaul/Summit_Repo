ALTER TABLE CSSTG_OWNER.PS_ITEM_SF MODIFY 
  NRA_TAXATION_SWTCH NULL
/

ALTER TABLE CSSTG_OWNER.PS_ITEM_SF MODIFY 
  NRA_TAXATION_SWTCH NOT NULL
  ENABLE VALIDATE
/

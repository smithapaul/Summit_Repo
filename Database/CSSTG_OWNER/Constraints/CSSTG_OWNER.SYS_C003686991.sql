ALTER TABLE CSSTG_OWNER.PS_ITEM_SF MODIFY 
  TAX_AUTHORITY_CD NULL
/

ALTER TABLE CSSTG_OWNER.PS_ITEM_SF MODIFY 
  TAX_AUTHORITY_CD NOT NULL
  ENABLE VALIDATE
/

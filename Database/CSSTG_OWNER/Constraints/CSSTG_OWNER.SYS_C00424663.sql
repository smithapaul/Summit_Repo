ALTER TABLE CSSTG_OWNER.PS_E_ADDRESSES MODIFY 
  LOOKUP_COL_LIST NULL
/

ALTER TABLE CSSTG_OWNER.PS_E_ADDRESSES MODIFY 
  LOOKUP_COL_LIST NOT NULL
  ENABLE VALIDATE
/

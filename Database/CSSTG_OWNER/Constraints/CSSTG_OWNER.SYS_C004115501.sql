ALTER TABLE CSSTG_OWNER.PS_CLASS_PRMSN MODIFY 
  SSR_OVRD_TIME_PERD NULL
/

ALTER TABLE CSSTG_OWNER.PS_CLASS_PRMSN MODIFY 
  SSR_OVRD_TIME_PERD NOT NULL
  ENABLE VALIDATE
/

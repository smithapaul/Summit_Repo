ALTER TABLE CSSTG_OWNER.PS_T_CLASS_PRMSN_OLD MODIFY 
  SSR_OVRD_TIME_PERD NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_CLASS_PRMSN_OLD MODIFY 
  SSR_OVRD_TIME_PERD NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSSTG_OWNER.PS_T_TRNS_CRSE_DTL MODIFY 
  TRNSFR_EQVLNCY_CMP NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_TRNS_CRSE_DTL MODIFY 
  TRNSFR_EQVLNCY_CMP NOT NULL
  ENABLE VALIDATE
/

ALTER TABLE CSSTG_OWNER.PS_T_TRNS_CRSE_SCH MODIFY 
  TRF_TAKEN_NOGPA NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_TRNS_CRSE_SCH MODIFY 
  TRF_TAKEN_NOGPA NOT NULL
  ENABLE VALIDATE
/

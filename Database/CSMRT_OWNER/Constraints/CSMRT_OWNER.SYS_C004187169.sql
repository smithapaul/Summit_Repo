ALTER TABLE CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR MODIFY 
  PARTITION_KEY NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR MODIFY 
  PARTITION_KEY NOT NULL
  ENABLE VALIDATE
/

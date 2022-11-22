DROP VIEW CSMRT_OWNER.UM_D_PERSON_ATHL_VW
/

--
-- UM_D_PERSON_ATHL_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_ATHL_VW
BEQUEATH DEFINER
AS 
SELECT R.PERSON_ID,
           nvl(SPORT,'-') SPORT,
           R.SRC_SYS_ID,
           EFFDT,
           R.PERSON_SID,
           R.PERSON_ID     EMPLID,
           R.INSTITUTION_CD,
           SPORT_SD,
           SPORT_LD,
           ATHL_PARTIC_CD,
           ATHL_PARTIC_SD,
           ATHL_PARTIC_LD,
           NCAA_ELIGIBLE,
           CUR_PARTICIPANT,
           DESCRLONG,
           ATHL_ORDER,
           R.DATA_ORIGIN,
           R.CREATED_EW_DTTM,
           R.LASTUPD_EW_DTTM
      FROM UM_R_PERSON_ASSOC  R
           left outer JOIN UM_D_PERSON_ATHL A 
             ON R.PERSON_ATHL_SID = A.PERSON_SID
            and R.INSTITUTION_CD = A.INSTITUTION_CD
            and A.DATA_ORIGIN <> 'D'
/

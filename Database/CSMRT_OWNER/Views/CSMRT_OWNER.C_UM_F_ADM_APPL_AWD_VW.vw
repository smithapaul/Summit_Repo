DROP VIEW CSMRT_OWNER.C_UM_F_ADM_APPL_AWD_VW
/

--
-- C_UM_F_ADM_APPL_AWD_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_F_ADM_APPL_AWD_VW
BEQUEATH DEFINER
AS 
WITH
        ADM
        --        AS (SELECT DISTINCT APPLCNT_SID, INSTITUTION_SID, SRC_SYS_ID
        AS
            (SELECT DISTINCT APPLCNT_SID,
                             INSTITUTION_SID,
                             INSTITUTION_CD,
                             SRC_SYS_ID
               FROM UM_F_ADM_APPL_STAT),
        AWD
        AS
            (SELECT PERSON_SID,
                    R.AWD_SID,
                    AWD_RCVD_DT,
                    R.SRC_SYS_ID,
                    --D.INSTITUTION_SID,
                    PERSON_ID,
                    R.INSTITUTION_CD,
                    R.AWD_CD,
                    --ACAD_CAR_CD, ACAD_CAR_SD, ACAD_PROG_CD, ACAD_PROG_SD, ACAD_PLAN_CD, ACAD_PLAN_SD, TERM_CD, TERM_SD,
                    DESCRFORMAL,
                    GRANTOR,
                    COMMENTS,
                    R.EFF_START_DT,
                    R.EFF_END_DT,
                    R.CURRENT_IND
               FROM PS_R_AWD  R
                    JOIN PS_D_AWD D
                        ON R.AWD_SID = D.AWD_SID AND R.DATA_ORIGIN <> 'D')
    SELECT ADM.APPLCNT_SID,
           ADM.INSTITUTION_SID,
           ADM.SRC_SYS_ID,
           NVL (AWD.AWD_SID, 2147483646)     AWD_SID,
           AWD_RCVD_DT,
           PERSON_ID,
           ADM.INSTITUTION_CD,
           AWD_CD,
           --ACAD_CAR_CD, ACAD_CAR_SD, ACAD_PROG_CD, ACAD_PROG_SD, ACAD_PLAN_CD, ACAD_PLAN_SD,TERM_CD, TERM_SD,
           DESCRFORMAL,
           GRANTOR,
           COMMENTS,
           case
                when EFF_START_DT = '01-JAN-1900' then to_date('01/01/1753', 'DD/MM/YYYY')
                else EFF_START_DT
           end EFF_START_DT,
           EFF_END_DT,
           CURRENT_IND
      FROM ADM
           LEFT OUTER JOIN AWD
               ON     ADM.APPLCNT_SID = AWD.PERSON_SID
                  --                AND ADM.INSTITUTION_SID = AWD.INSTITUTION_SID
                  AND ADM.INSTITUTION_CD = AWD.INSTITUTION_CD
                  AND ADM.SRC_SYS_ID = AWD.SRC_SYS_ID
     WHERE ROWNUM < 1000000000
/

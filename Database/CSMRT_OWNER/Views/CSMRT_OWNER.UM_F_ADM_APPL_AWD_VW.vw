CREATE OR REPLACE VIEW UM_F_ADM_APPL_AWD_VW
BEQUEATH DEFINER
AS 
WITH
        ADM
        AS
            (SELECT DISTINCT APPLCNT_SID,
                             PERSON_ID,
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
                    PERSON_ID,
                    R.INSTITUTION_CD,
                    R.AWD_CD,
                    DESCRFORMAL,
                    GRANTOR,
                    COMMENTS
--                    R.EFF_START_DT,
--                    R.EFF_END_DT,
--                    R.CURRENT_IND
               FROM PS_R_AWD  R
                    JOIN PS_D_AWD D
                        ON R.AWD_SID = D.AWD_SID AND R.DATA_ORIGIN <> 'D')
    select ADM.INSTITUTION_CD,
           ADM.PERSON_ID,
           AWD_RCVD_DT,
           AWD_CD,
           ADM.SRC_SYS_ID,
           ADM.APPLCNT_SID,
           ADM.INSTITUTION_SID,
           NVL (AWD.AWD_SID, 2147483646)     AWD_SID,
           DESCRFORMAL,
           GRANTOR,
           COMMENTS
--           EFF_START_DT,
--           EFF_END_DT,
--           CURRENT_IND
      FROM ADM
           LEFT OUTER JOIN AWD
               ON     ADM.APPLCNT_SID = AWD.PERSON_SID
                  --                AND ADM.INSTITUTION_SID = AWD.INSTITUTION_SID
                  AND ADM.INSTITUTION_CD = AWD.INSTITUTION_CD
                  AND ADM.SRC_SYS_ID = AWD.SRC_SYS_ID
     WHERE ROWNUM < 1000000000;

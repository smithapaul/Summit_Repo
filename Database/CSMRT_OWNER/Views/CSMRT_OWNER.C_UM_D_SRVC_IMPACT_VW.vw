DROP VIEW CSMRT_OWNER.C_UM_D_SRVC_IMPACT_VW
/

--
-- C_UM_D_SRVC_IMPACT_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_SRVC_IMPACT_VW
BEQUEATH DEFINER
AS 
WITH
        Q1
        AS
            (SELECT INSTITUTION,
                    SERVICE_IMPACT,
                    SRC_SYS_ID,
                    EFFDT,
                    EFF_STATUS,
                    SCC_IMPACT_TERM
                        SCC_IMPACT_TERM_FLG,
                    SCC_IMPACT_DATE
                        SCC_IMPACT_DATE_FLG,
                    POS_SRVC_IMPACT
                        POS_SRVC_IMPACT_FLG,
                    SYSTEM_FUNCTION
                        SYSTEM_FUNCTION_FLG,
                    DESCR,
                    DESCRSHORT,
                    CAST ('N' AS VARCHAR2 (1))
                        LOAD_ERROR,
                    DATA_ORIGIN,
                    CREATED_EW_DTTM,
                    LASTUPD_EW_DTTM,
                    CAST (1234 AS NUMBER (10))
                        BATCH_SID,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY INSTITUTION,
                                         SERVICE_IMPACT,
                                         SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC)
                        Q_ORDER
               FROM CSSTG_OWNER.PS_SRVC_IMPACT_TBL)
    SELECT ROW_NUMBER ()
               OVER (
                   ORDER BY Q1.INSTITUTION, Q1.SERVICE_IMPACT, Q1.SRC_SYS_ID)
               SRVC_IMPACT_SID,
           Q1.INSTITUTION,
           Q1.SERVICE_IMPACT,
           Q1.SRC_SYS_ID,
           NVL (I.INSTITUTION_SID, 2147483646)
               INSTITUTION_SID,
           Q1.EFFDT,
           Q1.EFF_STATUS,
           Q1.SCC_IMPACT_TERM_FLG,
           Q1.SCC_IMPACT_DATE_FLG,
           Q1.POS_SRVC_IMPACT_FLG,
           Q1.SYSTEM_FUNCTION_FLG,
           Q1.DESCR,
           Q1.DESCRSHORT,
           Q1.LOAD_ERROR,
           Q1.DATA_ORIGIN,
           Q1.CREATED_EW_DTTM,
           Q1.LASTUPD_EW_DTTM,
           Q1.BATCH_SID
      FROM Q1
           LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION I
               ON     Q1.INSTITUTION = I.INSTITUTION_CD
                  AND Q1.SRC_SYS_ID = I.SRC_SYS_ID
                  AND I.DATA_ORIGIN <> 'D'
     WHERE Q1.Q_ORDER = 1
    UNION ALL
    SELECT 2147483646                  SRVC_IMPACT_SID,
           '-'                         INSTITUTION,
           '-'                         SERVICE_IMPACT,
           'CS90'                      SRC_SYS_ID,
           2147483646                  INSTITUTION_SID,
           TO_DATE ('01-JAN-1900')     EFFDT,
           '-'                         EFF_STATUS,
           '-'                         SCC_IMPACT_TERM_FLG,
           '-'                         SCC_IMPACT_DATE_FLG,
           '-'                         POS_SRVC_IMPACT_FLG,
           '-'                         SYSTEM_FUNCTION_FLG,
           '-'                         DESCR,
           '-'                         DESCRSHORT,
           'N'                         LOAD_ERROR,
           'S'                         DATA_ORIGIN,
           SYSDATE                     CREATED_EW_DTTM,
           SYSDATE                     LASTUPD_EW_DTTM,
           1234                        BATCH_SID
      FROM DUAL
/

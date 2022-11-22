DROP VIEW CSMRT_OWNER.C_UM_D_SRVC_IND_VW
/

--
-- C_UM_D_SRVC_IND_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_SRVC_IND_VW
BEQUEATH DEFINER
AS 
WITH
        Q1
        AS
            (SELECT INSTITUTION,
                    SRVC_IND_CD,
                    SRC_SYS_ID,
                    EFFDT,
                    EFF_STATUS,
                    DESCR,
                    DESCRSHORT,
                    POS_SRVC_INDICATOR
                        POS_SRVC_IND_FLG,
                    SCC_HOLD_DISPLAY
                        SCC_HOLD_DISP_FLG,
                    SCC_SI_PERS
                        SCC_SI_PERS_FLG,
                    SCC_SI_ORG
                        SCC_SI_ORG_FLG,
                    SCC_DFLT_ACTDATE
                        SCC_DFLT_ACTDATE_FLG,
                    SCC_DFLT_ACTTERM
                        SCC_DFLT_ACTTERM_FLG,
                    DFLT_SRVC_IND_RSN,
                    SRV_IND_DCSD_FLAG
                        SRV_IND_DCSD_FLG,
                    LOAD_ERROR,
                    DATA_ORIGIN,
                    CREATED_EW_DTTM,
                    LASTUPD_EW_DTTM,
                    BATCH_SID,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY INSTITUTION, SRVC_IND_CD, SRC_SYS_ID
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
               FROM CSSTG_OWNER.PS_SRVC_IND_CD_TBL)
    SELECT ROW_NUMBER ()
               OVER (ORDER BY Q1.INSTITUTION, Q1.SRVC_IND_CD, Q1.SRC_SYS_ID)
               SRVC_IND_SID,
           Q1.INSTITUTION,
           Q1.SRVC_IND_CD,
           Q1.SRC_SYS_ID,
           CAST (NVL (I.INSTITUTION_SID, 2147483646) AS NUMBER (10))
               INSTITUTION_SID,
           Q1.EFFDT,
           Q1.EFF_STATUS,
           Q1.DESCR,
           Q1.DESCRSHORT,
           Q1.POS_SRVC_IND_FLG,
           Q1.SCC_HOLD_DISP_FLG,
           Q1.SCC_SI_PERS_FLG,
           Q1.SCC_SI_ORG_FLG,
           Q1.SCC_DFLT_ACTDATE_FLG,
           Q1.SCC_DFLT_ACTTERM_FLG,
           Q1.DFLT_SRVC_IND_RSN,
           Q1.SRV_IND_DCSD_FLG,
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
    SELECT 2147483646                  SRVC_IND_SID,
           '-'                         INSTITUTION,
           '-'                         SRVC_IND_CD,
           'CS90'                      SRC_SYS_ID,
           2147483646                  INSTITUTION_SID,
           TO_DATE ('01-JAN-1900')     EFFDT,
           '-'                         EFF_STATUS,
           '-'                         DESCR,
           '-'                         DESCRSHORT,
           '-'                         POS_SRVC_IND_FLG,
           '-'                         SCC_HOLD_DISP_FLG,
           '-'                         SCC_SI_PERS_FLG,
           '-'                         SCC_SI_ORG_FLG,
           '-'                         SCC_DFLT_ACTDATE_FLG,
           '-'                         SCC_DFLT_ACTTERM_FLG,
           '-'                         DFLT_SRVC_IND_RSN,
           '-'                         SRV_IND_DCSD_FLG,
           'N'                         LOAD_ERROR,
           'S'                         DATA_ORIGIN,
           SYSDATE                     CREATED_EW_DTTM,
           SYSDATE                     LASTUPD_EW_DTTM,
           1234                        BATCH_SID
      FROM DUAL
/

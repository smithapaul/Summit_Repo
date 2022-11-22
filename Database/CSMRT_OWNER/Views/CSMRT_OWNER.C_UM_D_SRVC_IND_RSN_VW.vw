DROP VIEW CSMRT_OWNER.C_UM_D_SRVC_IND_RSN_VW
/

--
-- C_UM_D_SRVC_IND_RSN_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_SRVC_IND_RSN_VW
BEQUEATH DEFINER
AS 
WITH
        Q1
        AS
            (SELECT INSTITUTION,
                    SRVC_IND_CD,
                    SRVC_IND_REASON,
                    SRC_SYS_ID,
                    EFFDT,
                    DESCR,
                    DESCRSHORT,
                    DEPTID,
                    LOAD_ERROR,
                    DATA_ORIGIN,
                    CREATED_EW_DTTM,
                    LASTUPD_EW_DTTM,
                    CAST (1234 AS NUMBER (10))
                        BATCH_SID,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY INSTITUTION,
                                         SRVC_IND_CD,
                                         SRVC_IND_REASON,
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
               FROM CSSTG_OWNER.PS_SRVC_IN_RSN_TBL),
        Q2
        AS
            (SELECT ROW_NUMBER ()
                        OVER (ORDER BY INSTITUTION, SRVC_IND_CD, SRC_SYS_ID)
                        SRVC_IND_SID,
                    INSTITUTION,
                    SRVC_IND_CD,
                    SRC_SYS_ID
               FROM (SELECT DISTINCT INSTITUTION, SRVC_IND_CD, SRC_SYS_ID
                       FROM CSSTG_OWNER.PS_SRVC_IND_CD_TBL))
    SELECT ROW_NUMBER ()
               OVER (ORDER BY
                         Q1.INSTITUTION,
                         Q1.SRVC_IND_CD,
                         Q1.SRVC_IND_REASON,
                         Q1.SRC_SYS_ID)
               SRVC_IND_SID,
           Q1.INSTITUTION,
           Q1.SRVC_IND_CD,
           Q1.SRVC_IND_REASON,
           Q1.SRC_SYS_ID,
           NVL (I.INSTITUTION_SID, 2147483646)
               INSTITUTION_SID,
           NVL (Q2.SRVC_IND_SID, 2147483646)
               SRVC_IND_SID,
           Q1.EFFDT,
           Q1.DESCR,
           Q1.DESCRSHORT,
           NVL (D.DEPT_SID, 2147483646)
               DEPT_SID,
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
           LEFT OUTER JOIN Q2
               ON     Q1.INSTITUTION = Q2.INSTITUTION
                  AND Q1.SRVC_IND_CD = Q2.SRVC_IND_CD
                  AND Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
           LEFT OUTER JOIN CSMRT_OWNER.PS_D_DEPT D
               ON     Q1.INSTITUTION = D.SETID
                  AND Q1.DEPTID = D.DEPT_ID
                  AND Q1.SRC_SYS_ID = D.SRC_SYS_ID
                  AND D.DATA_ORIGIN <> 'D'
     WHERE Q1.Q_ORDER = 1
    UNION ALL
    SELECT 2147483646                  SRVC_IND_RSN_SID,
           '-'                         INSTITUTION,
           '-'                         SRVC_IND_CD,
           '-'                         SRVC_IND_REASON,
           'CS90'                      SRC_SYS_ID,
           2147483646                  INSTITUTION_SID,
           2147483646                  SRVC_IND_SID,
           TO_DATE ('01-JAN-1900')     EFFDT,
           '-'                         DESCR,
           '-'                         DESCRSHORT,
           2147483646                  DEPT_SID,
           'N'                         LOAD_ERROR,
           'S'                         DATA_ORIGIN,
           SYSDATE                     CREATED_EW_DTTM,
           SYSDATE                     LASTUPD_EW_DTTM,
           1234                        BATCH_SID
      FROM DUAL
/

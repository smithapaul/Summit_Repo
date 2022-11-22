DROP VIEW CSMRT_OWNER.UM_F_ADM_APPL_EXT_CRSE_VW
/

--
-- UM_F_ADM_APPL_EXT_CRSE_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_ADM_APPL_EXT_CRSE_VW
BEQUEATH DEFINER
AS 
WITH
        A
        AS
            (SELECT DISTINCT APPLCNT_SID,
                             INSTITUTION_SID,
                             SRC_SYS_ID,
                             INSTITUTION_CD
               FROM UM_F_ADM_APPL_STAT),
        O
        AS
            (SELECT DISTINCT A.APPLCNT_SID,
                             A.INSTITUTION_SID,
                             NVL (O.EXT_ORG_SID, 2147483646)     EXT_ORG_SID,
                             A.SRC_SYS_ID,
                             A.INSTITUTION_CD
               FROM A
                    LEFT OUTER JOIN PS_F_EXT_ACAD_SUMM O
                        ON     A.APPLCNT_SID = O.PERSON_SID
                           AND A.INSTITUTION_SID = O.INSTITUTION_SID
                           AND A.SRC_SYS_ID = O.SRC_SYS_ID
                           AND NVL (O.DATA_ORIGIN, '-') <> 'D')
    SELECT O.APPLCNT_SID,
           O.INSTITUTION_SID,
           O.EXT_ORG_SID,
           O.SRC_SYS_ID,
           E.EXT_COURSE_NBR,
           E.PERSON_ID EMPLID,
           E.PERSON_ID,
           O.INSTITUTION_CD,
           E.EXT_ORG_ID,
           NVL (E.EXT_ACAD_CAR_SID, 2147483646)
               EXT_ACAD_CAR_SID,
           E.EXT_CRSE_TYPE,
           NVL (
               (SELECT MIN (X.XLATSHORTNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'EXT_CRSE_TYPE'
                       AND X.FIELDVALUE = E.EXT_CRSE_TYPE),
               '-')
               EXT_CRSE_TYPE_SD,
           NVL (
               (SELECT MIN (X.XLATLONGNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'EXT_CRSE_TYPE'
                       AND X.FIELDVALUE = E.EXT_CRSE_TYPE),
               '-')
               EXT_CRSE_TYPE_LD,
           NVL (E.TST_DATA_SRC_SID, 2147483646)
               TST_DATA_SRC_SID,
           E.EXT_DATA_NBR,
           E.BEGIN_DT,
           E.END_DT,
           NVL (E.EXT_TERM_SID, 2147483646)
               EXT_TERM_SID,
           E.UNT_TAKEN,
           E.GRADING_SCHEME,
           E.GRADING_SCHEME_SD,
           E.GRADING_SCHEME_LD,
           E.GRADING_BASIS,
           NVL (
               (SELECT MIN (X.XLATSHORTNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'GRADING_BASIS'
                       AND X.FIELDVALUE = E.GRADING_BASIS),
               '-')
               GRADING_BASIS_SD,
           NVL (
               (SELECT MIN (X.XLATLONGNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'GRADING_BASIS'
                       AND X.FIELDVALUE = E.GRADING_BASIS),
               '-')
               GRADING_BASIS_LD,
           E.COURSE_LEVEL,
           NVL (
               (SELECT MIN (X.XLATSHORTNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'COURSE_LEVEL'
                       AND X.FIELDVALUE = E.COURSE_LEVEL),
               '-')
               COURSE_LEVEL_SD,
           NVL (
               (SELECT MIN (X.XLATLONGNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'COURSE_LEVEL'
                       AND X.FIELDVALUE = E.COURSE_LEVEL),
               '-')
               COURSE_LEVEL_LD,
           E.CRSE_GRADE_INPUT,
           E.CRSE_GRADE_OFF,
           E.SCHOOL_SUBJECT,
           E.SCHOOL_CRSE_NBR,
           E.EXT_SUBJECT_AREA,
           NVL (E.EXT_TERM_YEAR_SID, 2147483646)
               EXT_TERM_YEAR_SID,
           E.EXT_COURSE_DESCR,
           NVL (E.ACAD_UNIT_TYPE_SID, 2147483646)
               ACAD_UNIT_TYPE_SID,
           NVL (E.EXT_ACAD_LVL_SID, 2147483646)
               EXT_ACAD_LVL_SID,
           E.TRANS_CREDIT_FLAG,
           E.CAN_TRNS_TYPE,
           NVL (
               (SELECT MIN (X.XLATSHORTNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'CAN_TRNS_TYPE'
                       AND X.FIELDVALUE = E.CAN_TRNS_TYPE),
               '-')
               CAN_TRNS_TYPE_SD,
           NVL (
               (SELECT MIN (X.XLATLONGNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'CAN_TRNS_TYPE'
                       AND X.FIELDVALUE = E.CAN_TRNS_TYPE),
               '-')
               CAN_TRNS_TYPE_LD,
           E.LASTUPDDTTM,
           E.LASTUPDOPRID
      FROM O
           LEFT OUTER JOIN UM_F_EXT_ACAD_CRSE E
               ON     O.APPLCNT_SID = E.PERSON_SID
                  AND O.INSTITUTION_SID = E.INSTITUTION_SID
                  AND O.EXT_ORG_SID = E.EXT_ORG_SID
                  AND O.SRC_SYS_ID = E.SRC_SYS_ID
                  AND NVL (E.DATA_ORIGIN, '-') <> 'D'
     WHERE ROWNUM < 1000000000
/

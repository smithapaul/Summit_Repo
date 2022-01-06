CREATE OR REPLACE VIEW C_UM_R_STDNT_ADVR_VW
BEQUEATH DEFINER
AS 
WITH
        ADV
        AS
            (SELECT /*+ parallel(8) inline */
                    EMPLID,
                    INSTITUTION,
                    EFFDT,
                    ADVISOR_ROLE,
                    STDNT_ADVISOR_NBR,
                    SRC_SYS_ID,
                    ACAD_PROG,
                    ADVISOR_ID,
                    ACAD_CAREER,
                    ACAD_PLAN,
                    APPROVE_ENRLMT,
                    APPROVE_GRAD,
                    GRAD_APPROVED,
                    COMMITTEE_ID,
                    COMM_PERS_CD,
                    RANK ()
                        OVER (
                            PARTITION BY EMPLID, INSTITUTION, SRC_SYS_ID
                            ORDER BY
                                EFFDT DESC, DECODE (ACAD_PLAN, '-', 1, 0))
                        ADV_ORDER
               FROM CSSTG_OWNER.PS_STDNT_ADVR_HIST H1
              WHERE DATA_ORIGIN <> 'D'),
        XL
        AS
            (SELECT /*+ inline */
                    FIELDNAME,
                    FIELDVALUE,
                    SRC_SYS_ID,
                    XLATLONGNAME,
                    XLATSHORTNAME
               FROM CSMRT_OWNER.UM_D_XLATITEM
              WHERE SRC_SYS_ID = 'CS90')
    SELECT NVL (I.INSTITUTION_SID, 2147483646)
               INSTITUTION_SID,
           NVL (P.PERSON_SID, 2147483646)
               PERSON_SID,
           ADV.ADVISOR_ROLE,
           ADV.STDNT_ADVISOR_NBR,
           ADV.SRC_SYS_ID,
           ADV.INSTITUTION
               INSTITUTION_CD,
           ADV.EMPLID,
           ADV.EFFDT,
           NVL (P2.PERSON_SID, 2147483646)
               STUDENT_ADVISOR_SID,
           NVL (C.ACAD_CAR_SID, 2147483646)
               ACAD_CAR_SID,
           NVL (G.ACAD_PROG_SID, 2147483646)
               ACAD_PROG_SID,
           NVL (L.ACAD_PLAN_SID, 2147483646)
               ACAD_PLAN_SID,
           ADV.APPROVE_ENRLMT,
           ADV.APPROVE_GRAD,
           ADV.GRAD_APPROVED,
           ADV.COMMITTEE_ID,
           ADV.COMM_PERS_CD,
           NVL (X1.XLATLONGNAME, '')
               ADVISOR_ROLE_LD,                                       -- Added
           NVL (X1.XLATSHORTNAME, '')
               ADVISOR_ROLE_SD,                                       -- Added
           P2.PERSON_NM
               STUDENT_ADVISOR_NM,
           ROW_NUMBER ()
               OVER (
                   PARTITION BY ADV.INSTITUTION,
                                ADV.EMPLID,
                                ADV.ACAD_PLAN,
                                ADV.SRC_SYS_ID                     -- Nov 2018
                   ORDER BY
                       DECODE (ADVISOR_ROLE,
                               'FAC', 1,
                               'ADVR', 2,
                               'PROF', 3,
                               'MADV', 4,
                               'HONR', 5,
                               'ATHL', 6,
                               'PROG', 7,
                               9),
                       (CASE
                            WHEN UPPER (P2.PERSON_NM) LIKE 'DEPARTMENT%'
                            THEN
                                999999
                            WHEN UPPER (P2.PERSON_NM) LIKE 'ADVISING%'
                            THEN
                                999999
                            WHEN UPPER (P2.PERSON_NM) LIKE 'PROGRAM%'
                            THEN
                                999999
                            ELSE
                                STDNT_ADVISOR_NBR
                        END))
               STUDENT_ADVISOR_ORDER,
           (CASE
                WHEN I.INSTITUTION_SID IS NULL THEN cast('K' as VARCHAR2(1))
                WHEN P.PERSON_SID IS NULL THEN cast('K' as VARCHAR2(1))
                ELSE 'N'
            END)
               LOAD_ERROR,
           cast('S' as VARCHAR2(1))    DATA_ORIGIN,
           SYSDATE
               CREATED_EW_DTTM,
           SYSDATE
               LASTUPD_EW_DTTM,
           1234
               BATCH_SID
      FROM ADV
           LEFT OUTER JOIN XL X1
               ON     X1.FIELDNAME = 'ADVISOR_ROLE'
                  AND X1.FIELDVALUE = ADV.ADVISOR_ROLE
                  AND X1.SRC_SYS_ID = ADV.SRC_SYS_ID
           LEFT OUTER JOIN PS_D_INSTITUTION I
               ON     ADV.INSTITUTION = I.INSTITUTION_CD
                  AND ADV.SRC_SYS_ID = I.SRC_SYS_ID
           LEFT OUTER JOIN UM_D_PERSON_AGG P
               ON ADV.EMPLID = P.PERSON_ID AND ADV.SRC_SYS_ID = P.SRC_SYS_ID
           LEFT OUTER JOIN UM_D_PERSON_AGG P2
               ON     ADV.ADVISOR_ID = P2.PERSON_ID
                  AND ADV.SRC_SYS_ID = P2.SRC_SYS_ID
           LEFT OUTER JOIN PS_D_ACAD_CAR C
               ON     ADV.INSTITUTION = C.INSTITUTION_CD
                  AND ADV.ACAD_CAREER = C.ACAD_CAR_CD
                  AND ADV.SRC_SYS_ID = C.SRC_SYS_ID
           LEFT OUTER JOIN UM_D_ACAD_PROG G
               ON     ADV.INSTITUTION = G.INSTITUTION_CD
                  AND ADV.ACAD_PROG = G.ACAD_PROG_CD
                  AND ADV.SRC_SYS_ID = G.SRC_SYS_ID
                  AND G.EFFDT_ORDER = 1
           LEFT OUTER JOIN UM_D_ACAD_PLAN L
               ON     ADV.INSTITUTION = L.INSTITUTION_CD
                  AND ADV.ACAD_PLAN = L.ACAD_PLAN_CD
                  AND ADV.SRC_SYS_ID = L.SRC_SYS_ID
                  AND L.EFFDT_ORDER = 1
     WHERE ADV.ADV_ORDER = 1;

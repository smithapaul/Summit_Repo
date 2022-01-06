CREATE OR REPLACE VIEW UM_F_ADM_APPL_COMM_VW
BEQUEATH DEFINER
AS 
WITH
        X
        AS
        (select 
                FIELDNAME, FIELDVALUE, SRC_SYS_ID,      -- Nov 2021 
                XLATLONGNAME, XLATSHORTNAME  
           from CSMRT_OWNER.UM_D_XLATITEM
          where DATA_ORIGIN <> 'D'
            and FIELDNAME in ('COMM_METHOD','OUTCOME_REASON','SCC_COMM_LANG','SCC_COMM_MTHD','SCC_COMM_PROC')
        ),
        V
        AS
        (SELECT DISTINCT COMMON_ID, ADM_APPL_NBR, VAR_DATA_SID
           FROM CSMRT_OWNER.PS_D_VAR_DATA
          WHERE ADMIN_FUNCTION = 'ADMP' 
            AND DATA_ORIGIN <> 'D'),
        A
        AS
            (SELECT DISTINCT APPLCNT_SID, ADM_APPL_NBR, INSTITUTION_SID, SRC_SYS_ID      -- Nov 2021 
               FROM CSMRT_OWNER.UM_F_ADM_APPL_STAT),
        C
        AS
            (SELECT F.PERSON_SID,
                    V.ADM_APPL_NBR,
                    F.SEQ_3C,
                    F.SRC_SYS_ID,
                    F.COMMON_ID,
                    F.SA_ID_TYPE,
                    F.COMM_DTTM,
                    F.INSTITUTION,
                    F.INSTITUTION_SID,
                    F.ADMIN_FUNC_SID,
                    F.COMM_CATEGORY,
                    F.COMM_CATEGORY_SD,
                    F.COMM_CATEGORY_LD,
                    F.COMM_CONTEXT,
                    F.COMM_CONTEXT_SD,
                    F.COMM_CONTEXT_LD,
                    F.COMM_METHOD,
                    F.DEPT_FUNC_SID,
                    --                   F.COMM_ID, -- Temporary until PERSON_ASSIGNED_ID added to CSMRT_OWNER.UM_F_COMM_PERSON!!!
                    F.COMM_DT,
                    F.COMPLETED_COMM_FLG,
                    F.PERSON_COMPLETED_SID,
                    F.PERSON_ASSIGNED_SID,
                    F.COMPLETED_DT,
                    F.UNSUCCESSFUL_FLG,
                    F.OUTCOME_REASON,
                    F.SCC_LETTER_CD,
                    F.SCC_LETTER_SD,
                    F.SCC_LETTER_LD,
                    F.LETTER_PRINTED_DT,
                    F.CHECKLIST_SEQ_3C,
                    F.CHECKLIST_SEQ,
                    F.COMMENT_PRINT_FLAG,
                    F.ORG_CONTACT,
                    F.PROCESS_INSTANCE,
                    F.VAR_DATA_SEQ,
                    F.VAR_DATA_SID,
                    F.JOINT_COMM_FLG,
                    F.SCC_COMM_LANG,
                    F.SCC_COMM_MTHD,
                    F.SCC_COMM_PROC,
                    SUBSTR (TO_CHAR (F.COMM_COMMENTS), 1, 4000)    COMM_COMMENTS
               FROM CSMRT_OWNER.UM_F_COMM_PERSON  F
--                    JOIN CSMRT_OWNER.PS_D_ADMIN_FUNC D
--                        ON     F.ADMIN_FUNC_SID = D.ADMIN_FUNC_SID
--                           AND D.ADMIN_FUNCTION IN ('ADMA', 'ADMP', 'EVNT')
                    JOIN V
                        ON F.VAR_DATA_SID = V.VAR_DATA_SID)
    SELECT A.APPLCNT_SID,
           A.ADM_APPL_NBR,
           A.INSTITUTION_SID,       -- Nov 2021 
           A.SRC_SYS_ID,
           C.SEQ_3C,
           C.COMMON_ID,
           C.SA_ID_TYPE,
           C.COMM_DTTM,
           C.INSTITUTION,
           NVL (C.ADMIN_FUNC_SID, 2147483646)          ADMIN_FUNC_SID,
           NVL (C.DEPT_FUNC_SID, 2147483646)           DEPT_FUNC_SID,
           NVL (C.PERSON_ASSIGNED_SID, 2147483646)     PERSON_ASSIGNED_SID,
           NVL (C.PERSON_COMPLETED_SID, 2147483646)    PERSON_COMPLETED_SID,
           NVL (C.VAR_DATA_SID, 2147483646)            VAR_DATA_SID,
           COMM_CATEGORY,
           COMM_CATEGORY_SD,
           COMM_CATEGORY_LD,
           COMM_CONTEXT,
           COMM_CONTEXT_SD,
           COMM_CONTEXT_LD,
           COMM_METHOD,
           NVL (X1.XLATSHORTNAME, '-') COMM_METHOD_SD,
           NVL (X1.XLATLONGNAME, '-') COMM_METHOD_LD,
           NVL ((SELECT PERSON_ID
                   FROM UM_D_PERSON_AGG
                  WHERE PERSON_SID = C.PERSON_ASSIGNED_SID),
                '-')                                   COMM_ID, -- Temporary until PERSON_ASSIGNED_ID added to CSMRT_OWNER.UM_F_COMM_PERSON!!! 
           COMM_DT,
           COMPLETED_COMM_FLG,
           COMPLETED_DT,
           UNSUCCESSFUL_FLG,
           OUTCOME_REASON,
           NVL (X2.XLATSHORTNAME, '-') OUTCOME_REASON_SD,
           NVL (X2.XLATLONGNAME, '-') OUTCOME_REASON_LD,
           SCC_LETTER_CD,
           SCC_LETTER_SD,
           SCC_LETTER_LD,
           LETTER_PRINTED_DT,
           CHECKLIST_SEQ_3C,
           CHECKLIST_SEQ,
           COMMENT_PRINT_FLAG,
           ORG_CONTACT,
           PROCESS_INSTANCE,
           VAR_DATA_SEQ,
           JOINT_COMM_FLG,
           SCC_COMM_LANG,
           NVL (X3.XLATSHORTNAME, '-') SCC_COMM_LANG_SD,
           NVL (X3.XLATLONGNAME, '-') SCC_COMM_LANG_LD,
           SCC_COMM_MTHD,
           NVL (X4.XLATSHORTNAME, '-') SCC_COMM_MTHD_SD,
           NVL (X4.XLATLONGNAME, '-') SCC_COMM_MTHD_LD,
           SCC_COMM_PROC,
           NVL (X5.XLATSHORTNAME, '-') SCC_COMM_PROC_SD,
           NVL (X5.XLATLONGNAME, '-') SCC_COMM_PROC_LD,
           COMM_COMMENTS
      FROM A
           LEFT OUTER JOIN C
               ON     A.APPLCNT_SID = C.PERSON_SID
                  AND A.ADM_APPL_NBR = C.ADM_APPL_NBR
                  AND A.SRC_SYS_ID = C.SRC_SYS_ID
           left outer join X X1
             on X1.FIELDNAME = 'COMM_METHOD'
            and X1.FIELDVALUE = C.COMM_METHOD
            and X1.SRC_SYS_ID = C.SRC_SYS_ID
           left outer join X X2
             on X2.FIELDNAME = 'OUTCOME_REASON'
            and X2.FIELDVALUE = C.OUTCOME_REASON
            and X2.SRC_SYS_ID = C.SRC_SYS_ID
           left outer join X X3
             on X3.FIELDNAME = 'SCC_COMM_LANG'
            and X3.FIELDVALUE = C.SCC_COMM_LANG
            and X3.SRC_SYS_ID = C.SRC_SYS_ID
           left outer join X X4
             on X4.FIELDNAME = 'SCC_COMM_MTHD'
            and X4.FIELDVALUE = C.SCC_COMM_MTHD
            and X4.SRC_SYS_ID = C.SRC_SYS_ID
           left outer join X X5
             on X5.FIELDNAME = 'SCC_COMM_PROC'
            and X5.FIELDVALUE = C.SCC_COMM_PROC
            and X5.SRC_SYS_ID = C.SRC_SYS_ID
     WHERE ROWNUM < 1000000000;

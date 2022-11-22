DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM_VW2
/

--
-- UM_F_FA_STDNT_AID_COMM_VW2  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM_VW2
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
           PERSON_ID,
           AID_YEAR,
           SEQ_3C,
           SRC_SYS_ID,
           INSTITUTION_SID,
           PERSON_SID,
           SA_ID_TYPE,
           COMM_DTTM,
           ADMIN_FUNC_SID,
           COMM_CATEGORY,
           COMM_CATEGORY_SD,
           COMM_CATEGORY_LD,
           COMM_CONTEXT,
           COMM_CONTEXT_SD,
           COMM_CONTEXT_LD,
           COMM_METHOD,
           COMM_METHOD_SD,
           COMM_METHOD_LD,
           DEPT_FUNC_SID,
           COMM_DT,
           PERSON_ASSIGNED_SID,
           PERSON_COMPLETED_SID,
           COMPLETED_COMM_FLG,
           COMPLETED_DT,
           UNSUCCESSFUL_FLG,
           OUTCOME_REASON,
           OUTCOME_REASON_SD,
           OUTCOME_REASON_LD,
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
           VAR_DATA_SID,
           JOINT_COMM_FLG,
           SCC_COMM_LANG,
           SCC_COMM_MTHD,
           SCC_COMM_PROC,
           COMM_COMMENTS,
           COMM_ORDER
      FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_COMM
     WHERE ROWNUM < 100000000
/

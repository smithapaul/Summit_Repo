DROP VIEW CSSTG_OWNER.PS_COMMUNICATION_VW
/

--
-- PS_COMMUNICATION_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.PS_COMMUNICATION_VW
BEQUEATH DEFINER
AS 
SELECT 
          COMMON_ID,
          SEQ_3C,
          SRC_SYS_ID,
          SA_ID_TYPE,
          COMM_DTTM,
          INSTITUTION,
          ADMIN_FUNCTION,
          COMM_CATEGORY,
          COMM_CONTEXT,
          COMM_METHOD,
          INCLUDE_ENCL,
          DEPTID,
          COMM_ID,
          COMM_DT,
          COMM_BEGIN_TM,
          COMM_END_TM,
          COMPLETED_COMM,
          COMPLETED_ID,
          COMPLETED_DT,
          COMM_DIRECTION,
          UNSUCCESSFUL,
          OUTCOME_REASON,
          SCC_LETTER_CD,
          LETTER_PRINTED_DT,
          LETTER_PRINTED_TM,
          CHECKLIST_SEQ_3C,
          CHECKLIST_SEQ,
          COMMENT_PRINT_FLAG,
          ORG_CONTACT,
          ORG_DEPARTMENT,
          ORG_LOCATION,
          PROCESS_INSTANCE,
          EXT_ORG_ID,
          VAR_DATA_SEQ,
          CAST (TRIM (SUBSTR (COMM_COMMENTS, 1, 2000)) AS VARCHAR2 (2000)) COMM_COMMENTS,
          EMPLID_RELATED,
          JOINT_COMM,
          SCC_COMM_LANG,
          SCC_COMM_MTHD,
          SCC_COMM_PROC,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM PS_COMMUNICATION
/

DROP VIEW CSMRT_OWNER.UM_A_SRVC_IND_VW
/

--
-- UM_A_SRVC_IND_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_A_SRVC_IND_VW
BEQUEATH DEFINER
AS 
SELECT AUDIT_OPRID,
           AUDIT_STAMP,
           AUDIT_ACTN,
           PERSON_ID,
           SRVC_IND_DTTM,
           SRC_SYS_ID,
           INSTITUTION_CD,
           INSTITUTION_SID,
           PERSON_SID,
--           SRVC_IND_SID,
--           SRVC_IND_RSN_SID,
           AMOUNT,
           AUDIT_ACTN_SD,
           AUDIT_ACTN_LD,
           CONTACT,
           CONTACT_ID,
           DEPTID,
           OPRID,
           PLACED_METHOD,
           PLACED_PERSON,
           PLACED_PERSON_ID,
           PLACED_PROCESS,
           POS_SRVC_INDICATOR,
           PROCESS_INSTANCE,
           RELEASE_PROCESS,
           SCC_SI_END_TERM,
           SCC_SI_END_DT,
           SRVC_IND_ACT_TERM,
           SRVC_IND_ACTIVE_DT,
           SRVC_IND_REFRNCE,
  SRVC_IND_CD,  
  SRVC_IND_SD, 
  SRVC_IND_LD, 
  SRVC_IND_REASON,
  SRVC_IND_REASON_SD, 
  SRVC_IND_REASON_LD, 
           COMM_COMMENTS,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM CSMRT_OWNER.UM_A_SRVC_IND
/

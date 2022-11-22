DROP VIEW CSMRT_OWNER.UM_D_PERSON_SRVC_IND_VW
/

--
-- UM_D_PERSON_SRVC_IND_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_SRVC_IND_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
           PERSON_ID,
           SRVC_IND_DTTM,
           SRC_SYS_ID,
           PERSON_ID     EMPLID,                                    -- Temp!!!
           EFFDT,
           INSTITUTION_SID,
           PERSON_SID,
           AMOUNT,
           CONTACT,
           CONTACT_ID,
           DFLT_SRVC_IND_RSN,
           OPRID,
           PLACED_METHOD,
           PLACED_PERSON,
           PLACED_PERSON_ID,
           PLACED_PROCESS,
           POS_SRVC_IMPACT_FLG,
           POS_SRVC_IND_FLG,
           POS_SRVC_INDICATOR,
           SCC_DFLT_ACTDATE_FLG,
           SCC_DFLT_ACTTERM_FLG,
           SCC_HOLD_DISP_FLG,
           SCC_IMPACT_DATE_FLG,
           SCC_IMPACT_TERM_FLG,
           SCC_SI_END_TERM,
           SCC_SI_END_DT,
           SCC_SI_END_TERM_SDESC,
           SCC_SI_END_TERM_DESC,
           SCC_SI_ORG_FLG,
           SCC_SI_PERS_FLG,
           SERVICE_IMPACT,
           SERVICE_IMPACT_SD,
           SERVICE_IMPACT_LD,
           SRV_IND_DCSD_FLG,
           SRVC_IND_ACT_TERM,
           SRVC_IND_ACTIVE_DT,
           SRVC_IND_ACT_TERM_SDESC,
           SRVC_IND_ACT_TERM_DESC,
           SRVC_IND_CD,
           SRVC_IND_SD,
           SRVC_IND_LD,
           SRVC_IND_REASON,
           SRVC_IND_REASON_SD,
           SRVC_IND_REASON_LD,
           SRVC_IND_REFRNCE,
           SYSTEM_FUNCTION_FLG,
           TERM_CATEGORY,
           COMM_COMMENTS,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_SRVC_IND
     where DATA_ORIGIN <> 'D'       -- Temp!!!!!!!!!!!!!!!!!!
/

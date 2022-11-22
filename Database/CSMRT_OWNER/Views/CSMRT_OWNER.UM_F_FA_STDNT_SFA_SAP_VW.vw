DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP_VW
/

--
-- UM_F_FA_STDNT_SFA_SAP_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
          ACAD_CAR_CD,
          AID_YEAR,
          PERSON_ID,
          PROCESS_DTTM,
          SRC_SYS_ID,
          INSTITUTION_SID,
          ACAD_CAR_SID,
          PERSON_SID,
          ACAD_PROG_SID,
          ACAD_PLAN_SID,
          STDNT_CAR_NBR,
          SFA_SAP_MAX_ATTUNT,
          SFA_SAP_MAX_ATTFRM,
          SFA_SAP_MAX_ATTMPT,
          SFA_INUSE_FLAG2,
          SFA_SAP_STATUS_C2,
          SFA_FAIL_FLAG2,
          CUR_GPA,
          CUM_GPA,
          SFA_SAP_MIN_CUM,
          SFA_INUSE_FLAG5,
          SFA_SAP_STATUS_C5,
          SFA_FAIL_FLAG5,
          SFA_CUM_ATT_UNITS,
          SFA_CUM_ERN_UNITS,
          SFA_SAP_CUM_EARNPC,
          SFA_INUSE_FLAG7,
          SFA_SAP_STATUS_C7,
          SFA_FAIL_FLAG7,
          SFA_SAP_2YR_GPA,
          SFA_SAP_CUMERN_PCT,
          SFA_SAP_STATUS,
          SFA_SAP_STATUS_SD,
          SFA_SAP_STATUS_LD,        -- Added June 2016 
          SFA_SAP_STAT_CALC,
          SFA_SAP_STAT_CALC_SD,
          SFA_SAP_STAT_CALC_LD,     -- Added June 2016 
          SFA_UPDT_OPRID,
          SFA_UPDT_DTTM,
          SFA_PROCESS_OPRID,
          SFA_SAP_PROCMSG,
          SFA_SAP_COMMENTS,
          SFA_SAP_ORDER,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM UM_F_FA_STDNT_SFA_SAP
    WHERE ROWNUM < 100000000
/

DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS_VW
/

--
-- UM_F_FA_STDNT_WS_AWARDS_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_WS_AWARDS_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_FA_STDNT_WS_AWARDS MIN=100000) */
          INSTITUTION_CD,
          ACAD_CAR_CD,
          AID_YEAR,
          PERSON_ID,
          ITEM_TYPE,
          SEQNO,
          EFFDT,
          SRC_SYS_ID,
          INSTITUTION_SID,
          ACAD_CAR_SID,
          PERSON_SID,
          ITEM_TYPE_SID,
          ACCOUNT,
          ACTION_DT,
          AWARD_STATUS,
          COMMENTS_MSGS,
          COMMUNITY_SERVICE,
          COMMUNITY_SERVICE_LD,     -- Sept 2016 
          EFF_STATUS,
          EMAILID,
          EMPLOYER,
          EMPL_RCD,
          END_DT,
          HOURLY_RT,
          JOBID,
          JOB_REC_EFFDT,
          JOB_REC_EFFSEQ,
          PHONE,
          SUPERVISOR_NAME,
          UM_EXEMPT,
          UM_SEC_ACCOUNT,
          UM_THIRD_ACCOUNT,
          UM_FOURTH_ACCOUNT,
          WS_PLACEMENT_STAT,
          WS_PLACEMENT_STAT_LD,
          WS_PLACEMENT_DT,
          START_DATE,
          END_DATE,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM UM_F_FA_STDNT_WS_AWARDS
    WHERE ROWNUM < 10000000
/

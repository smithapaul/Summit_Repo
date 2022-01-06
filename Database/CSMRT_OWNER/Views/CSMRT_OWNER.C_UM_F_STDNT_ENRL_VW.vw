CREATE OR REPLACE VIEW C_UM_F_STDNT_ENRL_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_STDNT_ENRL MIN=100000) */
           SESSION_SID,
           CLASS_NUM,
           PERSON_SID,
           SRC_SYS_ID,
           INSTITUTION_CD,
           ACAD_CAR_CD,
           TERM_CD,
           SESSION_CD,
           PERSON_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           TERM_SID,
           CLASS_SID,
           ENRLMT_MAX_TERM_SID,                              -- Added Mar 2018
           ENRLMT_MIN_TERM_SID,                             -- Added June 2018
           ENRLMT_MIN_PERSON_TERM_SID,                      -- Added June 2018
           ENRLMT_PREV_TERM_SID,                            -- Added June 2018
           ENRLMT_STAT_SID,
           ENRLMT_REAS_SID,
           ENRL_ACN_LAST_SID,
           ENRL_ACN_RSN_LAST_SID,
           GRADE_SID,
           GRADE_INPUT_SID,
           MID_TERM_GRADE_SID,
           REPEAT_SID,
           ASSOCIATED_CLASS,
           CLASS_CD,
           CLASS_PRMSN_NBR,
           EARN_CREDIT_FLG,
           ENRL_ACTN_PRC_LAST,
           ENRL_ACTN_PRC_LAST_SD,
           ENRL_ACTN_PRC_LAST_LD,
           ENRL_STATUS_DT,
           ENRL_ADD_DT,
           ENRL_DROP_DT,
           ENRLMT_MAX_TERM_CD,                               -- Added Apr 2015
           ENRLMT_MIN_TERM_CD,                              -- Added June 2018
           ENRLMT_MIN_PERSON_TERM_CD,                       -- Added June 2018
           ENRLMT_PREV_TERM_CD,                             -- Added June 2018
           ENRL_REQ_SOURCE,
           ENRL_REQ_SOURCE_SD,
           ENRL_REQ_SOURCE_LD,
           GRADE_DT,
           --APPROVAL_DATE,                                     --Added OCT 2021 
           GRADE_CATEGORY,
           GRADE_BASIS_CD,
           GRADE_BASIS_SD,
           GRADE_BASIS_LD,
           GRADE_BASIS_DT,
           GRADE_BASIS_OVRD_FLG,
           INCLUDE_IN_GPA_FLG,
           LAST_UPD_ENREQ_SRC,
           LAST_UPD_ENREQ_SRC_SD,
           LAST_UPD_ENREQ_SRC_LD,
           MANDATORY_GRD_BAS_FLG,
           NOTIFY_STDNT_CHNG,
           NOTIFY_STDNT_CHNG_SD,
           NOTIFY_STDNT_CHNG_LD,
           REPEAT_DT,
           REPEAT_FLG,
           RSRV_CAP_NBR,
           STDNT_POSITIN,
           TSCRPT_NOTE_ID,                                         -- Aug 2016
           TSCRPT_NOTE_DESCR,                                      -- Aug 2016
           TSCRPT_NOTE_EXISTS,                                     -- Aug 2016
           TSCRPT_NOTE254,                                         -- Aug 2016
           UM_STD_COMPL_CRSE_FLG,                                  -- May 2016
           UM_STD_NEVER_ATTND_FLG,                                 -- May 2016
           UM_STD_LST_DT_ATTD,                                     -- May 2016
           UNITS_ATTEMPTED_CD,
           UNITS_ATTEMPTED_SD,
           UNITS_ATTEMPTED_LD,
           VALID_ATTEMPT_FLG,
           AUDIT_CNT,                                        -- Added APR 2015
           BILLING_UNIT,
           CE_CREDITS,
           CE_FTE,
           DAY_CREDITS,
           DAY_FTE,
           (CE_CREDITS + DAY_CREDITS)     TOT_CREDITS,
           (CE_FTE + DAY_FTE)             TOT_FTE,
           CRSE_CNT,
           DROP_CNT,
           ENROLL_CNT,
           ERN_UNIT,
           GRADE_PTS,
           GRADE_PTS_FA,
           GRADE_PTS_PER_UNIT,
           IFTE_CNT,
           ONLINE_CNT,                                       -- Added APR 2015
           ONLINE_CREDITS,                                   -- Added JUN 2015
           PRGRS_UNIT,
           PRGRS_FA_UNIT,
           TAKEN_UNIT,
           WAIT_CNT,
           LAST_UPD_DT_STMP,
           LAST_UPD_TM_STMP,
           LAST_ENRL_DT_STMP,
           LAST_ENRL_TM_STMP,
           LAST_DROP_DT_STMP,
           LAST_DROP_TM_STMP,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           ROWNUM                         ROW_NBR -- Added Jan 2017 for outer join performance
      FROM UM_F_STDNT_ENRL;

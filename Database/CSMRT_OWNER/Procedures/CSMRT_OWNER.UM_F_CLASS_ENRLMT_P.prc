CREATE OR REPLACE PROCEDURE             "UM_F_CLASS_ENRLMT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_CLASS_ENRLMT
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_CLASS_ENRLMT';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_CLASS_ENRLMT');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS_ENRLMT disable constraint PK_UM_F_CLASS_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_CLASS_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_CLASS_ENRLMT';
insert /*+ append */ into UM_F_CLASS_ENRLMT
with   
CLASS as (
select /*+ INLINE PARALLEL(8) */
       CLASS_SID,
       TERM_CD,
       SESSION_CD,
       SRC_SYS_ID,
       INSTITUTION_CD,
       CLASS_NUM,
       row_number() over (partition by TERM_CD, SRC_SYS_ID, INSTITUTION_CD, CLASS_NUM
                              order by CLASS_SID) CLASS_ORDER
  from CSMRT_OWNER.UM_D_CLASS
 where DATA_ORIGIN <> 'D'),
ROS as (
select /*+ INLINE PARALLEL(8) */
    R.STRM,
    R.CLASS_NBR,
    R.EMPLID,
    R.ACAD_CAREER,
    R.SRC_SYS_ID,
    R.GRADING_SCHEME,
    R.GRADING_BASIS_ENRL,
    R.CRSE_GRADE_INPUT,
    T.APPROVAL_DATE,
    row_number() over (partition by R.STRM, R.CLASS_NBR, R.EMPLID, R.ACAD_CAREER, R.SRC_SYS_ID
                           order by T.GRD_RSTR_TYPE_SEQ desc) ROSTER_ORDER
    from CSSTG_OWNER.PS_GRADE_ROSTER R
    join CSSTG_OWNER.PS_GRADE_RSTR_TYPE T
      on R.STRM = T.STRM
     and R.CLASS_NBR = T.CLASS_NBR
     and R.GRD_RSTR_TYPE_SEQ = T.GRD_RSTR_TYPE_SEQ
     and R.SRC_SYS_ID = T.SRC_SYS_ID
     and T.GRADE_ROSTER_TYPE = 'MID'
     and R.DATA_ORIGIN <> 'D'
     and T.DATA_ORIGIN <> 'D'
),
NOTE as (
select /*+ INLINE PARALLEL(8) */
       EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR, SRC_SYS_ID,
       TSCRPT_NOTE254,
       row_number() over (partition by EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR, SRC_SYS_ID order by TSCRPT_NOTE_SEQ desc) NOTE_ORDER
  from CSSTG_OWNER.PS_STDNT_ENRL_NOTE
 where DATA_ORIGIN <> 'D'
),
TSC as (
select SETID, TSCRPT_NOTE_ID, SRC_SYS_ID,
       DESCR,
       row_number() over (partition by SETID, TSCRPT_NOTE_ID, SRC_SYS_ID order by EFFDT desc) TSC_ORDER
  from CSSTG_OWNER.PS_TSCRPT_NOTE_TBL
 where DATA_ORIGIN <> 'D'
),
ENRL as (
select /*+ INLINE PARALLEL(8) */
    E.EMPLID,
    E.ACAD_CAREER,
    E.INSTITUTION,
    E.STRM,
    E.CLASS_NBR,
    E.SRC_SYS_ID,
    CRSE_CAREER,
    SESSION_CODE,
    STDNT_ENRL_STATUS,
    ENRL_STATUS_REASON,
    ENRL_ACTION_LAST,
    ENRL_ACTN_RSN_LAST,
    ENRL_ACTN_PRC_LAST,
    STATUS_DT,
    ENRL_ADD_DT,
    ENRL_DROP_DT,
    UNT_TAKEN,
    UNT_PRGRSS,
    UNT_PRGRSS_FA,
    UNT_BILLING,
    CRSE_COUNT,
    E.GRADING_BASIS_ENRL,
    ROS.GRADING_SCHEME GRADING_SCHEME_ROS,
    ROS.GRADING_BASIS_ENRL GRADING_BASIS_ENRL_ROS,
    GRADING_BASIS_DT,
    OVRD_GRADING_BASIS,
    CRSE_GRADE_OFF,
    E.CRSE_GRADE_INPUT,
    ROS.CRSE_GRADE_INPUT CRSE_GRADE_INPUT_ROS,
    ROS.APPROVAL_DATE,
    GRADE_DT,
    REPEAT_CODE,
    REPEAT_DT,
    CLASS_PRMSN_NBR,
    ASSOCIATED_CLASS,
    STDNT_POSITIN,
    EARN_CREDIT,
    INCLUDE_IN_GPA,
    UNITS_ATTEMPTED,
    GRADE_POINTS,
    GRADE_POINTS_FA,
    GRD_PTS_PER_UNIT,
    MANDATORY_GRD_BAS,
    RSRV_CAP_NBR,
    NOTIFY_STDNT_CHNG,
    REPEAT_CANDIDATE,
    E.TSCRPT_NOTE_ID,               -- Aug 2016
    TSC.DESCR TSCRPT_NOTE_DESCR,    -- Aug 2016
    E.TSCRPT_NOTE_EXISTS,           -- Aug 2016
    NOTE.TSCRPT_NOTE254,            -- Aug 2016
    VALID_ATTEMPT,
    GRADE_CATEGORY,
    UNT_EARNED,
    LAST_UPD_DT_STMP,
    LAST_UPD_TM_STMP,
    LAST_ENRL_DT_STMP,
    LAST_ENRL_TM_STMP,
    LAST_DROP_DT_STMP,
    LAST_DROP_TM_STMP,
    ENRL_REQ_SOURCE,
    LAST_UPD_ENREQ_SRC,
    GRADING_SCHEME_ENR,
    nvl(A.UM_STD_COMPL_CRSE,'-') UM_STD_COMPL_CRSE,
    nvl(A.UM_STD_NEVER_ATTND,'-') UM_STD_NEVER_ATTND,
    UM_STD_LST_DT_ATTD
  from CSSTG_OWNER.PS_STDNT_ENRL E
  left outer join ROS
    on E.STRM = ROS.STRM
   and E.CLASS_NBR = ROS.CLASS_NBR
   and E.EMPLID = ROS.EMPLID
   and E.ACAD_CAREER = ROS.ACAD_CAREER
   and E.SRC_SYS_ID = ROS.SRC_SYS_ID
   and ROS.ROSTER_ORDER = 1
  left outer join NOTE
    on E.EMPLID = NOTE.EMPLID
   and E.ACAD_CAREER = NOTE.ACAD_CAREER
   and E.INSTITUTION = NOTE.INSTITUTION
   and E.STRM = NOTE.STRM
   and E.CLASS_NBR = NOTE.CLASS_NBR
   and E.SRC_SYS_ID = NOTE.SRC_SYS_ID
   and NOTE.NOTE_ORDER = 1
  left outer join TSC
    on E.INSTITUTION = TSC.SETID
   and E.TSCRPT_NOTE_ID = TSC.TSCRPT_NOTE_ID
   and E. SRC_SYS_ID = TSC.SRC_SYS_ID
   and TSC.TSC_ORDER = 1
  left outer join CSSTG_OWNER.PS_UM_STD_LST_ATND A
    on E.EMPLID = A.EMPLID
   and E.ACAD_CAREER = A.ACAD_CAREER
   and E.INSTITUTION = A.INSTITUTION
   and E.STRM = A.STRM
   and E.CLASS_NBR = A.CLASS_NBR
   and E.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
 where E.DATA_ORIGIN <> 'D'
--   and E.STRM >= '1010'
),
FACT as (
select /*+ PARALLEL(8) */
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       ENRL.CLASS_NBR CLASS_NUM,
       ENRL.SRC_SYS_ID,
       ENRL.INSTITUTION INSTITUTION_CD,
       ENRL.ACAD_CAREER ACAD_CAR_CD,
       ENRL.STRM TERM_CD,
       nvl(CLASS.SESSION_CD,ENRL.SESSION_CODE) SESSION_CD,
       ENRL.EMPLID PERSON_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(S.SESSION_SID,2147483646) SESSION_SID,
       nvl(CLASS.CLASS_SID,2147483646) CLASS_SID,
       nvl(A.ENRL_REQ_ACTN_SID,2147483646) ENRL_ACN_LAST_SID,
       nvl(R.ENRL_ACT_RSN_SID,2147483646) ENRL_ACN_RSN_LAST_SID,
       nvl(ES.ENRLMT_STAT_SID,2147483646) ENRLMT_STAT_SID,
       nvl(ER.ENRLMT_REAS_SID,2147483646) ENRLMT_REAS_SID,
       nvl(G1.GRADE_SID,2147483646) GRADE_SID,
       nvl(G2.GRADE_SID,2147483646) GRADE_INPUT_SID,
       nvl(G3.GRADE_SID,2147483646) MID_TERM_GRADE_SID,
       nvl(RP.REPEAT_SID,2147483646) REPEAT_SID,
       ENRL.ASSOCIATED_CLASS,
       ENRL.CLASS_PRMSN_NBR,
       ENRL.EARN_CREDIT EARN_CREDIT_FLG,
       ENRL.ENRL_ACTN_PRC_LAST,
       ENRL.STATUS_DT ENRL_STATUS_DT,
       ENRL.ENRL_ADD_DT,
       ENRL.ENRL_DROP_DT,
       ENRL.ENRL_REQ_SOURCE,
       ENRL.GRADE_DT,
       ENRL.APPROVAL_DATE,
       ENRL.GRADE_CATEGORY,
       ENRL.GRADING_BASIS_ENRL GRADE_BASIS_CD,
       ENRL.GRADING_BASIS_DT GRADE_BASIS_DT,
       ENRL.OVRD_GRADING_BASIS GRADE_BASIS_OVRD_FLG,
       ENRL.INCLUDE_IN_GPA INCLUDE_IN_GPA_FLG,
       ENRL.LAST_UPD_ENREQ_SRC,
       ENRL.MANDATORY_GRD_BAS MANDATORY_GRD_BAS_FLG,
       ENRL.NOTIFY_STDNT_CHNG,
       ENRL.REPEAT_DT,
       ENRL.REPEAT_CANDIDATE REPEAT_FLG,
       ENRL.RSRV_CAP_NBR,
       ENRL.STDNT_POSITIN,
       ENRL.TSCRPT_NOTE_ID,
       ENRL.TSCRPT_NOTE_DESCR,
       ENRL.TSCRPT_NOTE_EXISTS,
       ENRL.TSCRPT_NOTE254,
       ENRL.UNITS_ATTEMPTED UNITS_ATTEMPTED_CD,
       ENRL.VALID_ATTEMPT VALID_ATTEMPT_FLG,
       ENRL.UNT_BILLING BILLING_UNIT,
       ENRL.UNT_EARNED ERN_UNIT,
       ENRL.UNT_PRGRSS PRGRS_UNIT,
       ENRL.UNT_PRGRSS_FA PRGRS_FA_UNIT,
       ENRL.UNT_TAKEN TAKEN_UNIT,
       ENRL.CRSE_COUNT CRSE_CNT,
       ENRL.GRADE_POINTS GRADE_PTS,
       ENRL.GRADE_POINTS_FA GRADE_PTS_FA,
       ENRL.GRD_PTS_PER_UNIT GRADE_PTS_PER_UNIT,
       ENRL.LAST_UPD_DT_STMP,
       ENRL.LAST_UPD_TM_STMP,
       ENRL.LAST_ENRL_DT_STMP,
       ENRL.LAST_ENRL_TM_STMP,
       ENRL.LAST_DROP_DT_STMP,
       ENRL.LAST_DROP_TM_STMP,
       ENRL.UM_STD_COMPL_CRSE UM_STD_COMPL_CRSE_FLG,
       ENRL.UM_STD_NEVER_ATTND UM_STD_NEVER_ATTND_FLG,
       ENRL.UM_STD_LST_DT_ATTD
  from ENRL
  left outer join PS_D_TERM T
    on ENRL.INSTITUTION = T.INSTITUTION_CD
   and ENRL.ACAD_CAREER = T.ACAD_CAR_CD
   and ENRL.STRM = T.TERM_CD
   and ENRL.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
--  left outer join UM_D_PERSON_AGG P     -- Change to inner join???  -- Timing ok, or use PS_D_PERSON???
  left outer join PS_D_PERSON P     -- Change to inner join???  -- Timing ok, or use PS_D_PERSON???
    on ENRL.EMPLID = P.PERSON_ID
   and ENRL.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTITUTION I
    on ENRL.INSTITUTION = I.INSTITUTION_CD
   and ENRL.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C
    on ENRL.INSTITUTION = C.INSTITUTION_CD
   and ENRL.ACAD_CAREER = C.ACAD_CAR_CD
   and ENRL.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CLASS
    on ENRL.STRM = CLASS.TERM_CD
   and ENRL.SRC_SYS_ID = CLASS.SRC_SYS_ID
   and ENRL.INSTITUTION = CLASS.INSTITUTION_CD
   and ENRL.CLASS_NBR = CLASS.CLASS_NUM
   and CLASS.CLASS_ORDER = 1
  left outer join PS_D_SESSION S
    on ENRL.INSTITUTION = S.INSTITUTION_CD
   and ENRL.ACAD_CAREER = S.ACAD_CAR_CD
   and ENRL.STRM = S.TERM_CD
   and nvl(CLASS.SESSION_CD,ENRL.SESSION_CODE) = S.SESSION_CD
   and ENRL.SRC_SYS_ID = S.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_ENRL_ACTION A
    on ENRL.ENRL_ACTION_LAST = A.ENRL_REQ_ACTION
   and ENRL.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
  left outer join PS_D_ENRL_RSN R
    on ENRL.INSTITUTION = R.SETID
   and ENRL.ACAD_CAREER = R.ACAD_CAR_CD
   and ENRL.ENRL_ACTION_LAST = R.ENRL_ACTION
   and ENRL.ENRL_ACTN_RSN_LAST = ENRL_ACT_RSN
   and ENRL.SRC_SYS_ID = R.SRC_SYS_ID
   and R.DATA_ORIGIN <> 'D'
  left outer join PS_D_ENRLMT_STAT ES
    on ENRL.STDNT_ENRL_STATUS = ES.ENRLMT_STAT_ID
   and ENRL.SRC_SYS_ID = ES.SRC_SYS_ID
   and ES.DATA_ORIGIN <> 'D'
  left outer join PS_D_ENRLMT_REAS ER
    on ENRL.ENRL_STATUS_REASON = ER.ENRLMT_REAS_ID
   and ENRL.SRC_SYS_ID = ER.SRC_SYS_ID
   and ER.DATA_ORIGIN <> 'D'
  left outer join PS_D_GRADE G1
    on ENRL.INSTITUTION = G1.SETID
   and ENRL.CRSE_GRADE_OFF = G1.GRADE_CD
   and ENRL.GRADING_SCHEME_ENR = G1.GRADE_SCHEME_CD
   and ENRL.GRADING_BASIS_ENRL = G1.GRADE_BASIS_CD
   and ENRL.SRC_SYS_ID = G1.SRC_SYS_ID
   and G1.DATA_ORIGIN <> 'D'
  left outer join PS_D_GRADE G2
    on ENRL.INSTITUTION = G2.SETID
   and ENRL.CRSE_GRADE_INPUT = G2.GRADE_CD
   and ENRL.GRADING_SCHEME_ENR = G2.GRADE_SCHEME_CD
   and ENRL.GRADING_BASIS_ENRL = G2.GRADE_BASIS_CD
   and ENRL.SRC_SYS_ID = G2.SRC_SYS_ID
   and G2.DATA_ORIGIN <> 'D'
  left outer join PS_D_GRADE G3
    on ENRL.INSTITUTION = G3.SETID
   and ENRL.CRSE_GRADE_INPUT_ROS = G3.GRADE_CD
   and ENRL.GRADING_SCHEME_ROS = G3.GRADE_SCHEME_CD
   and ENRL.GRADING_BASIS_ENRL_ROS = G3.GRADE_BASIS_CD
   and ENRL.SRC_SYS_ID = G3.SRC_SYS_ID
   and G3.DATA_ORIGIN <> 'D'
  left outer join PS_D_REPEAT RP
    on ENRL.INSTITUTION = RP.SETID
   and C.REPEAT_SCHEME = RP.REPEAT_SCHEME_CD
   and ENRL.REPEAT_CODE = RP.REPEAT_CD
   and ENRL.SRC_SYS_ID = RP.SRC_SYS_ID
   and RP.DATA_ORIGIN <> 'D'
),
E1 as (
SELECT /*+ INLINE PARALLEL(8) */
                       FACT.TERM_SID,                       -- PK
                       FACT.PERSON_SID,                     -- PK
                       FACT.CLASS_NUM,                      -- PK   -- Instead of CLASS_SID when CLASS_SID = 2147483646
                       FACT.SRC_SYS_ID,                     -- PK
                       FACT.INSTITUTION_SID,
                       FACT.ACAD_CAR_SID,
                       FACT.SESSION_SID,
                       FACT.CLASS_SID,
                       FACT.INSTITUTION_CD,
                       CLASS.CRSE_CD,
                       CLASS.CRSE_OFFER_NUM,
--                       CLASS.TERM_CD,
                       FACT.TERM_CD,                        -- May 2017
                       CLASS.SESSION_CD,
                       CLASS.CLASS_SECTION_CD,
                       CLASS.CATALOG_NBR,
                       CAR.ACAD_CAR_CD as CLASS_ACAD_CAR_CD,
                       FACT.PERSON_ID,
                       FACT.ACAD_CAR_CD,
                       FACT.ENRLMT_STAT_SID,
                       FACT.GRADE_SID,
                       FACT.GRADE_INPUT_SID,
                       FACT.MID_TERM_GRADE_SID,
                       FACT.REPEAT_SID,
                       FACT.REPEAT_DT,
                       FACT.ASSOCIATED_CLASS,
                       FACT.CLASS_PRMSN_NBR,
                       FACT.EARN_CREDIT_FLG,
                       FACT.ENRL_ACTN_PRC_LAST,
                       FACT.ENRL_STATUS_DT,
                       FACT.ENRLMT_REAS_SID,
                       FACT.ENRL_ACN_LAST_SID,
                       FACT.ENRL_ACN_RSN_LAST_SID,
                       FACT.ENRL_ADD_DT,
                       FACT.ENRL_DROP_DT,
                       FACT.ENRL_REQ_SOURCE,
                       STAT.ENRLMT_STAT_ID,
                       FACT.GRADE_DT,
                       FACT.APPROVAL_DATE,
                       FACT.GRADE_CATEGORY,
                       FACT.GRADE_BASIS_CD,        -- Added Mar 2014.
                       FACT.GRADE_BASIS_DT,
                       FACT.GRADE_BASIS_OVRD_FLG,
                       FACT.INCLUDE_IN_GPA_FLG,
                       FACT.LAST_UPD_ENREQ_SRC,
                       FACT.MANDATORY_GRD_BAS_FLG,
                       FACT.NOTIFY_STDNT_CHNG,
                       FACT.RSRV_CAP_NBR,
                       FACT.STDNT_POSITIN,
                       FACT.TSCRPT_NOTE_ID,       -- Aug 2016
                       FACT.TSCRPT_NOTE_DESCR,    -- Aug 2016
                       FACT.TSCRPT_NOTE_EXISTS,   -- Aug 2016
                       FACT.TSCRPT_NOTE254,       -- Aug 2016
                       FACT.UM_STD_COMPL_CRSE_FLG,
                       FACT.UM_STD_NEVER_ATTND_FLG,
                       FACT.UM_STD_LST_DT_ATTD,
                       FACT.UNITS_ATTEMPTED_CD,
                       FACT.VALID_ATTEMPT_FLG,
                       FACT.BILLING_UNIT,
                       FACT.PRGRS_FA_UNIT,
                       FACT.REPEAT_FLG,
                       FACT.GRADE_PTS,
                       FACT.GRADE_PTS_FA,
                       FACT.GRADE_PTS_PER_UNIT,
                       FACT.TAKEN_UNIT,
                       NVL(FACT.CRSE_CNT, 0) CRSE_CNT,
                       FACT.PRGRS_UNIT,
                       FACT.ERN_UNIT,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID <> 'E'       -- Add UMLOW placeholder fix?
                          THEN 0
                          WHEN FACT.GRADE_BASIS_CD = 'AUD'
                          THEN 1
                          ELSE 0
                       END AUDIT_CNT,                           -- Added APR 2015
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID <> 'E'       -- Add UMLOW placeholder fix?
                          THEN 0
                          WHEN FACT.INSTITUTION_CD = 'UMBOS' AND FACT.SESSION_CD LIKE '%CE%'    -- Nov 2020
                          THEN FACT.TAKEN_UNIT
                          WHEN FACT.INSTITUTION_CD = 'UMDAR' AND FACT.SESSION_CD <> '1'         -- Oct 2017
                          THEN FACT.TAKEN_UNIT
                          WHEN FACT.INSTITUTION_CD = 'UMLOW' AND  CAR.ACAD_CAR_CD = 'CSCE'
                          THEN FACT.TAKEN_UNIT
                          ELSE 0
                       END CE_CREDITS,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID <> 'E'       -- Add UMLOW placeholder fix?
                          THEN 0
                          WHEN FACT.INSTITUTION_CD = 'UMBOS' AND FACT.SESSION_CD = '1' -- and I.INSTRCTN_MODE_CD not in ('EH','PH','WH')
                          THEN FACT.TAKEN_UNIT
                          WHEN FACT.INSTITUTION_CD = 'UMDAR' AND FACT.SESSION_CD = '1'
                          THEN FACT.TAKEN_UNIT
                          WHEN (FACT.INSTITUTION_CD = 'UMLOW' AND CAR.ACAD_CAR_CD IN ('UGRD', 'GRAD'))
                          THEN FACT.TAKEN_UNIT
                          ELSE 0
                       END DAY_CREDITS,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID = 'E'       -- Add UMLOW placeholder fix?
                          THEN 1
                          ELSE 0
                       END ENROLL_CNT,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID = 'D'
                          THEN 1
                          ELSE 0
                       END DROP_CNT,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID = 'W'
                          THEN 1
                          ELSE 0
                       END WAIT_CNT,
                       CASE
                          WHEN STAT.ENRLMT_STAT_ID <> 'E'
                          THEN 0
--                          WHEN I.INSTRCTN_MODE_CD in ('OL','OS','WH')
                          WHEN FACT.INSTITUTION_CD <> 'UMDAR' and I.INSTRCTN_MODE_CD in ('OL','OS','WH')            -- Aug 2020
                          THEN 1
                          WHEN FACT.INSTITUTION_CD = 'UMDAR' and CLASS.CLASS_SECTION_CD between '7100' and '7199'   -- Aug 2020
                          THEN 1
                          ELSE 0
                       END ONLINE_CNT,                           -- Added APR 2015
                       FACT.LAST_UPD_DT_STMP,
                       FACT.LAST_UPD_TM_STMP,
                       FACT.LAST_ENRL_DT_STMP,
                       FACT.LAST_ENRL_TM_STMP,
                       FACT.LAST_DROP_DT_STMP,
                       FACT.LAST_DROP_TM_STMP,
                       'N' LOAD_ERROR,
                       'S' DATA_ORIGIN,
                       SYSDATE CREATED_EW_DTTM,
                       SYSDATE LASTUPD_EW_DTTM,
                       1234 BATCH_SID
--                FROM   PS_F_CLASS_ENRLMT FACT,
                FROM   FACT,
                       PS_D_INSTRCTN_MODE I,
                       UM_D_CLASS CLASS,
                       PS_D_ENRLMT_STAT STAT,
                       PS_D_ACAD_CAR CAR
               WHERE   FACT.CLASS_SID = CLASS.CLASS_SID
                 AND   FACT.ENRLMT_STAT_SID = STAT.ENRLMT_STAT_SID
                 AND   CLASS.ACAD_CAR_SID = CAR.ACAD_CAR_SID
                 AND   CLASS.INSTRCTN_MODE_SID = I.INSTRCTN_MODE_SID
),
E2 as (
   SELECT   /*+ INLINE PARALLEL(8) */
            E1.TERM_SID,
            E1.PERSON_SID,
            E1.CLASS_NUM,
            E1.SRC_SYS_ID,
            E1.INSTITUTION_CD,
            E1.ACAD_CAR_CD,
            E1.TERM_CD,
            E1.SESSION_CD,
            E1.CLASS_SECTION_CD,
            E1.PERSON_ID,
            E1.INSTITUTION_SID,
            E1.ACAD_CAR_SID,
            E1.SESSION_SID,
            E1.CLASS_SID,
            E1.ENRLMT_STAT_SID,
            E1.ENRLMT_REAS_SID,
            E1.ENRL_ACN_LAST_SID,
            E1.ENRL_ACN_RSN_LAST_SID,
            E1.GRADE_SID,
            E1.GRADE_INPUT_SID,
            E1.MID_TERM_GRADE_SID,
            E1.REPEAT_SID,
            E1.ASSOCIATED_CLASS,
            E1.CLASS_PRMSN_NBR,
            E1.EARN_CREDIT_FLG,
            E1.ENRL_ACTN_PRC_LAST,
            E1.ENRL_STATUS_DT,
            E1.ENRL_ADD_DT,
            E1.ENRL_DROP_DT,
            E1.ENRL_REQ_SOURCE,
            E1.GRADE_DT,
            E1.APPROVAL_DATE,
            E1.GRADE_CATEGORY,
            E1.GRADE_BASIS_CD,        -- Added Mar 2014.
            E1.GRADE_BASIS_DT,
            E1.GRADE_BASIS_OVRD_FLG,
            E1.INCLUDE_IN_GPA_FLG,
            E1.LAST_UPD_ENREQ_SRC,
            E1.MANDATORY_GRD_BAS_FLG,
            E1.NOTIFY_STDNT_CHNG,
            E1.REPEAT_DT,
            E1.REPEAT_FLG,
            E1.RSRV_CAP_NBR,
            E1.STDNT_POSITIN,
            E1.TSCRPT_NOTE_ID,       -- Aug 2016
            E1.TSCRPT_NOTE_DESCR,    -- Aug 2016
            E1.TSCRPT_NOTE_EXISTS,   -- Aug 2016
            E1.TSCRPT_NOTE254,       -- Aug 2016
            E1.UM_STD_COMPL_CRSE_FLG,
            E1.UM_STD_NEVER_ATTND_FLG,
            E1.UM_STD_LST_DT_ATTD,
            E1.UNITS_ATTEMPTED_CD,
            E1.VALID_ATTEMPT_FLG,
            E1.BILLING_UNIT,
            case when nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA' then 0 else E1.AUDIT_CNT end AUDIT_CNT,             -- Added APR 2015
            case when nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA' then 0 else E1.CE_CREDITS end CE_CREDITS,             -- Added APR 2015
            E1.CRSE_CNT,
            case when nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA' then 0 else E1.DAY_CREDITS end DAY_CREDITS,             -- Added APR 2015
            E1.DROP_CNT,
            case when nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA' then 0 else E1.ENROLL_CNT end ENROLL_CNT,
            sum(case when not(nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA') then ENROLL_CNT else 0 end)
                over (partition by E1.TERM_SID, E1.PERSON_SID, E1.SRC_SYS_ID) ENROLL_CLASS_CNT,  -- Added APR 2015
            E1.ERN_UNIT,
            E1.GRADE_PTS,
            E1.GRADE_PTS_FA,
            E1.GRADE_PTS_PER_UNIT,
            E1.ENROLL_CNT HEAD_CNT,             -- May 2017
            CASE
               WHEN  E1.ENRLMT_STAT_ID <> 'E'
               THEN  0
               WHEN  E1.CLASS_ACAD_CAR_CD in ('LAW','UGRD')            -- Dartmouth fix 11/19/10       PS_D_ACAD_CAR
               THEN  E1.TAKEN_UNIT / 15
               WHEN  E1.CLASS_ACAD_CAR_CD = 'GRAD'
               THEN  E1.TAKEN_UNIT / 9
               WHEN  substr(trim(E1.CATALOG_NBR), 1, 1)  < '5'
               THEN  E1.TAKEN_UNIT / 15
               WHEN  substr(trim(E1.CATALOG_NBR), 1, 1) >= '5'
               THEN  E1.TAKEN_UNIT / 9
               ELSE  E1.TAKEN_UNIT / 15
            END IFTE_CNT,
            E1.PRGRS_UNIT,
            E1.PRGRS_FA_UNIT,
            E1.TAKEN_UNIT,
            E1.WAIT_CNT,
            case when nvl(V.CRSE_ATTR,'-') = 'LPLA' and nvl(V.CRSE_ATTR_VALUE,'-') = 'LPLA' then 0 else E1.ONLINE_CNT end ONLINE_CNT,             -- Added APR 2015
            E1.LAST_UPD_DT_STMP,
            E1.LAST_UPD_TM_STMP,
            E1.LAST_ENRL_DT_STMP,
            E1.LAST_ENRL_TM_STMP,
            E1.LAST_DROP_DT_STMP,
            E1.LAST_DROP_TM_STMP,
            E1.LOAD_ERROR,
            E1.DATA_ORIGIN,
            E1.CREATED_EW_DTTM,
            E1.LASTUPD_EW_DTTM,
            E1.BATCH_SID
   FROM     E1
--            JOIN PS_D_PERSON P
--              ON E1.PERSON_SID = P.PERSON_SID
--             AND E1.SRC_SYS_ID = P.SRC_SYS_ID
            LEFT OUTER JOIN UM_D_CLASS_ATTR_VAL V
              ON E1.CRSE_CD = V.CRSE_CD
             AND E1.CRSE_OFFER_NUM = V.CRSE_OFFER_NUM
             AND E1.TERM_CD = V.TERM_CD
             AND E1.SESSION_CD = V.SESSION_CD
             AND E1.CLASS_SECTION_CD = V.CLASS_SECTION_CD
             AND E1.SRC_SYS_ID = V.SRC_SYS_ID
             AND V.CRSE_ATTR = 'LPLA'
             AND V.CRSE_ATTR_VALUE = 'LPLA'
--            WHERE P.PERSON_NM not like '%XXX%'
)
   SELECT   /*+ INLINE PARALLEL(8) */
            E2.TERM_SID,
            E2.PERSON_SID,
            E2.CLASS_NUM,
            E2.SRC_SYS_ID,
            E2.INSTITUTION_CD,
            E2.ACAD_CAR_CD,
            E2.TERM_CD,
            E2.SESSION_CD,
            E2.CLASS_SECTION_CD,
            E2.PERSON_ID,
            E2.INSTITUTION_SID,
            E2.ACAD_CAR_SID,
            E2.SESSION_SID,
            E2.CLASS_SID,
            E2.ENRLMT_STAT_SID,
            E2.ENRLMT_REAS_SID,
            E2.ENRL_ACN_LAST_SID,
            E2.ENRL_ACN_RSN_LAST_SID,
            E2.GRADE_SID,
            E2.GRADE_INPUT_SID,
            E2.MID_TERM_GRADE_SID,
            E2.REPEAT_SID,
            E2.ASSOCIATED_CLASS,
            E2.CLASS_PRMSN_NBR,
            E2.EARN_CREDIT_FLG,
            E2.ENRL_ACTN_PRC_LAST,
            E2.ENRL_STATUS_DT,
            E2.ENRL_ADD_DT,
            E2.ENRL_DROP_DT,
            E2.ENRL_REQ_SOURCE,
            E2.GRADE_DT,
            E2.GRADE_CATEGORY,
            E2.GRADE_BASIS_CD,        -- Added Mar 2014.
            E2.GRADE_BASIS_DT,
            E2.GRADE_BASIS_OVRD_FLG,
            E2.INCLUDE_IN_GPA_FLG,
            E2.LAST_UPD_ENREQ_SRC,
            E2.MANDATORY_GRD_BAS_FLG,
            E2.NOTIFY_STDNT_CHNG,
            E2.REPEAT_DT,
            E2.REPEAT_FLG,
            E2.RSRV_CAP_NBR,
            E2.STDNT_POSITIN,
            E2.TSCRPT_NOTE_ID,       -- Aug 2016
            E2.TSCRPT_NOTE_DESCR,    -- Aug 2016
            E2.TSCRPT_NOTE_EXISTS,   -- Aug 2016
            E2.TSCRPT_NOTE254,       -- Aug 2016
            E2.UM_STD_COMPL_CRSE_FLG,
            E2.UM_STD_NEVER_ATTND_FLG,
            E2.UM_STD_LST_DT_ATTD,
            E2.UNITS_ATTEMPTED_CD,
            E2.VALID_ATTEMPT_FLG,
            E2.BILLING_UNIT,
            E2.AUDIT_CNT,             -- Added APR 2015
            E2.CE_CREDITS,             -- Added APR 2015
            CASE
               WHEN ACAD_CAR_CD = 'GRAD'
               THEN CE_CREDITS / 9
               ELSE CE_CREDITS / 15
            END CE_FTE,
            E2.CRSE_CNT,
            E2.DAY_CREDITS,             -- Added APR 2015
            CASE
               WHEN ACAD_CAR_CD = 'GRAD'
               THEN DAY_CREDITS / 9
               ELSE DAY_CREDITS / 15
            END DAY_FTE,
            E2.DROP_CNT,
            E2.ENROLL_CNT,
            E2.ERN_UNIT,
            E2.GRADE_PTS,
            E2.GRADE_PTS_FA,
            E2.GRADE_PTS_PER_UNIT,
            E2.HEAD_CNT,                -- May 2017
            E2.IFTE_CNT,
            E2.PRGRS_UNIT,
            E2.PRGRS_FA_UNIT,
            E2.TAKEN_UNIT,
            E2.WAIT_CNT,
            E2.ONLINE_CNT,             -- Added APR 2015
--            max(decode(ENROLL_CLASS_CNT,0,'-',E2.TERM_CD)) over (partition by E2.INSTITUTION_CD, E2.ACAD_CAR_CD, E2.PERSON_SID, E2.SRC_SYS_ID) ENRLMT_MAX_TERM_CD,  -- Added APR 2015
            '-' ENRLMT_MAX_TERM_CD,              -- Removed May 2015
            E2.LAST_UPD_DT_STMP,
            E2.LAST_UPD_TM_STMP,
            E2.LAST_ENRL_DT_STMP,
            E2.LAST_ENRL_TM_STMP,
            E2.LAST_DROP_DT_STMP,
            E2.LAST_DROP_TM_STMP,
            E2.LOAD_ERROR,
            E2.DATA_ORIGIN,
            E2.CREATED_EW_DTTM,
            E2.LASTUPD_EW_DTTM,
            E2.BATCH_SID,
            E2.APPROVAL_DATE         --Added Oct 2021
   FROM     E2
  WHERE     E2.PERSON_SID <> 2147483646
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_CLASS_ENRLMT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_CLASS_ENRLMT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_CLASS_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS_ENRLMT enable constraint PK_UM_F_CLASS_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_CLASS_ENRLMT');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_CLASS_ENRLMT_P;
/

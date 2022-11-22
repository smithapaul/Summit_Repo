DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_ENRL_P
/

--
-- UM_F_STDNT_ENRL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_ENRL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_ENRL
--V01 12/14/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ENRL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ENRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ENRL');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL disable constraint PK_UM_F_STDNT_ENRL';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ENRL';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_STDNT_ENRL
  with X as (
select /*+ inline */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
E1 as (
select /*+ INLINE PARALLEL(8) */
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID,  -- June 2018
       sum(ENROLL_CNT) TOT_ENRL_CNT
  from UM_F_CLASS_ENRLMT
 group by INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID),
E2 as (
select /*+ INLINE PARALLEL(8) */
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID,     -- June 2018
       nvl(max(TERM_CD) over (partition by INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, SRC_SYS_ID),'-') ENRLMT_MAX_TERM_CD,  -- Mar 2018
       nvl(min(TERM_CD) over (partition by INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, SRC_SYS_ID),'-') ENRLMT_MIN_TERM_CD,  -- June 2018
       nvl(min(TERM_CD) over (partition by INSTITUTION_CD, PERSON_ID, SRC_SYS_ID),'-') ENRLMT_MIN_PERSON_TERM_CD,  -- June 2018
       nvl(max(TERM_CD) over (partition by INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, SRC_SYS_ID     -- June 2018
                                  order by TERM_CD
                              rows between unbounded preceding and 1 preceding),'-') ENRLMT_PREV_TERM_CD,
       nvl(max(TERM_CD) over (partition by INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, SRC_SYS_ID     -- June 2018
                                  order by TERM_CD
                              rows between unbounded preceding and 2 preceding),'-') ENRLMT_PREV_TERM_CD2
  from E1
 where TOT_ENRL_CNT > 0),
E3 as (
select /*+ INLINE PARALLEL(8) */
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID,     -- June 2018
       ENRLMT_MAX_TERM_CD,  -- Mar 2018
       ENRLMT_MIN_TERM_CD,  -- June 2018
       ENRLMT_MIN_PERSON_TERM_CD,  -- June 2018
       case when substr(TERM_CD,-2,2) = '50' and substr(ENRLMT_PREV_TERM_CD,-2,2) = '40'
            then ENRLMT_PREV_TERM_CD2
            when substr(TERM_CD,-2,2) = '10' and substr(ENRLMT_PREV_TERM_CD,-2,2) = '50'    -- Need???
            then ENRLMT_PREV_TERM_CD2
            else ENRLMT_PREV_TERM_CD
        end ENRLMT_PREV_TERM_CD,
       ENRLMT_PREV_TERM_CD ENRLMT_PREV_TERM_CD1,
       ENRLMT_PREV_TERM_CD2
  from E2),
E4 as (
select /*+ INLINE PARALLEL(8) */ distinct
       T.INSTITUTION_CD, T.ACAD_CAR_CD, T.TERM_CD, T.PERSON_ID, T.SRC_SYS_ID,     -- June 2018
       nvl(max(E3.ENRLMT_MAX_TERM_CD) over (partition by T.INSTITUTION_CD, T.ACAD_CAR_CD, T.PERSON_ID, T.SRC_SYS_ID),'-') ENRLMT_MAX_TERM_CD,   -- July 2018
       nvl(min(E3.ENRLMT_MIN_TERM_CD) over (partition by T.INSTITUTION_CD, T.ACAD_CAR_CD, T.PERSON_ID, T.SRC_SYS_ID),'-') ENRLMT_MIN_TERM_CD,   -- July 2018
       nvl(min(E3.ENRLMT_MIN_PERSON_TERM_CD) over (partition by T.INSTITUTION_CD, T.PERSON_ID, T.SRC_SYS_ID),'-') ENRLMT_MIN_PERSON_TERM_CD,    -- July 2018
       nvl(max(case when T.TERM_CD > E3.ENRLMT_MAX_TERM_CD and substr(E3.ENRLMT_MAX_TERM_CD,-2,2) = '50'    -- Need???
                    then E3.ENRLMT_PREV_TERM_CD1
                    when T.TERM_CD = E3.ENRLMT_MIN_TERM_CD
                    then '-'
                    else greatest(E3.ENRLMT_PREV_TERM_CD, E3.ENRLMT_MIN_TERM_CD) end)
                    over (partition by T.INSTITUTION_CD, T.ACAD_CAR_CD, T.PERSON_ID, T.SRC_SYS_ID     -- June 2018
                              order by T.TERM_CD),'-') ENRLMT_PREV_TERM_CD
  from UM_F_STDNT_TERM T
  left outer join E3
    on T.INSTITUTION_CD = E3.INSTITUTION_CD
   and T.ACAD_CAR_CD = E3.ACAD_CAR_CD
   and T.TERM_CD >= E3.TERM_CD
   and T.PERSON_ID = E3.PERSON_ID
   and T.SRC_SYS_ID = E3.SRC_SYS_ID
),
TERM as (
   SELECT /*+ INLINE PARALLEL(8) */ distinct
            A.TERM_SID,
            A.PERSON_SID,
            nvl(CLASS_ENROLL.CLASS_NUM,0) CLASS_NUM,
            A.SRC_SYS_ID,
            A.INSTITUTION_CD INSTITUTION_CD,                        -- Fixed DEC 2015
            A.ACAD_CAR_CD ACAD_CAR_CD,                              -- Fixed DEC 2015
            A.TERM_CD TERM_CD,                                      -- Fixed DEC 2015
            nvl(CLASS_ENROLL.SESSION_CD,'-') SESSION_CD,
            A.PERSON_ID PERSON_ID,                                  -- Fixed DEC 2015
            A.INSTITUTION_SID,
            A.ACAD_CAR_SID,
            nvl(CLASS_ENROLL.SESSION_SID,2147483646) SESSION_SID,
            nvl(CLASS_ENROLL.CLASS_SID,2147483646) CLASS_SID,
            nvl(CLASS_ENROLL.ENRLMT_STAT_SID,2147483646) ENRLMT_STAT_SID,
            nvl(CLASS_ENROLL.ENRLMT_REAS_SID,2147483646) ENRLMT_REAS_SID,
            nvl(CLASS_ENROLL.ENRL_ACN_LAST_SID,2147483646) ENRL_ACN_LAST_SID,
            nvl(CLASS_ENROLL.ENRL_ACN_RSN_LAST_SID,2147483646) ENRL_ACN_RSN_LAST_SID,
            nvl(CLASS_ENROLL.GRADE_SID,2147483646) GRADE_SID,
            nvl(CLASS_ENROLL.GRADE_INPUT_SID,2147483646) GRADE_INPUT_SID,
            nvl(CLASS_ENROLL.MID_TERM_GRADE_SID,2147483646) MID_TERM_GRADE_SID,
            nvl(CLASS_ENROLL.REPEAT_SID,2147483646) REPEAT_SID,
            E4.ENRLMT_MAX_TERM_CD,                  -- Mar 2018
            E4.ENRLMT_MIN_TERM_CD,                  -- June 2018
            E4.ENRLMT_MIN_PERSON_TERM_CD,           -- June 2018
            E4.ENRLMT_PREV_TERM_CD,                 -- June 2018
            CLASS_ENROLL.ASSOCIATED_CLASS,
            CLASS_ENROLL.CLASS_PRMSN_NBR,
            CLASS_ENROLL.EARN_CREDIT_FLG,
            CLASS_ENROLL.ENRL_ACTN_PRC_LAST,
            CLASS_ENROLL.ENRL_STATUS_DT,
            CLASS_ENROLL.ENRL_ADD_DT,
            CLASS_ENROLL.ENRL_DROP_DT,
            CLASS_ENROLL.ENRL_REQ_SOURCE,
            CLASS_ENROLL.GRADE_DT,
            CLASS_ENROLL.GRADE_CATEGORY,
            CLASS_ENROLL.GRADE_BASIS_CD,        -- Added Mar 2014.
            CLASS_ENROLL.GRADE_BASIS_DT,
            CLASS_ENROLL.GRADE_BASIS_OVRD_FLG,
            CLASS_ENROLL.INCLUDE_IN_GPA_FLG,
            CLASS_ENROLL.LAST_UPD_ENREQ_SRC,
            CLASS_ENROLL.MANDATORY_GRD_BAS_FLG,
            CLASS_ENROLL.NOTIFY_STDNT_CHNG,
            CLASS_ENROLL.REPEAT_DT,
            CLASS_ENROLL.REPEAT_FLG,
            CLASS_ENROLL.RSRV_CAP_NBR,
            CLASS_ENROLL.STDNT_POSITIN,
            CLASS_ENROLL.TSCRPT_NOTE_ID,           -- Aug 2016
            CLASS_ENROLL.TSCRPT_NOTE_DESCR,        -- Aug 2016
            CLASS_ENROLL.TSCRPT_NOTE_EXISTS,       -- Aug 2016
            CLASS_ENROLL.TSCRPT_NOTE254,           -- Aug 2016
            CLASS_ENROLL.UM_STD_COMPL_CRSE_FLG,    -- May 2016
            CLASS_ENROLL.UM_STD_NEVER_ATTND_FLG,   -- May 2016
            CLASS_ENROLL.UM_STD_LST_DT_ATTD,       -- May 2016
            CLASS_ENROLL.UNITS_ATTEMPTED_CD,
            CLASS_ENROLL.VALID_ATTEMPT_FLG,
            CLASS_ENROLL.BILLING_UNIT,
            CLASS_ENROLL.AUDIT_CNT,             -- Added APR 2015
            CLASS_ENROLL.CE_CREDITS,
            CLASS_ENROLL.CE_FTE,
            CLASS_ENROLL.CRSE_CNT,
            CLASS_ENROLL.DAY_CREDITS,
            CLASS_ENROLL.DAY_FTE,
            CLASS_ENROLL.DROP_CNT,
            CLASS_ENROLL.ENROLL_CNT,
            CLASS_ENROLL.ERN_UNIT,
            CLASS_ENROLL.GRADE_PTS,
            CLASS_ENROLL.GRADE_PTS_FA,
            CLASS_ENROLL.GRADE_PTS_PER_UNIT,
            CLASS_ENROLL.IFTE_CNT,
            CLASS_ENROLL.ONLINE_CNT,             -- Added APR 2015
            CLASS_ENROLL.PRGRS_UNIT,
            CLASS_ENROLL.PRGRS_FA_UNIT,
            CLASS_ENROLL.TAKEN_UNIT,
            CLASS_ENROLL.WAIT_CNT,
            CLASS_ENROLL.LAST_UPD_DT_STMP,
            CLASS_ENROLL.LAST_UPD_TM_STMP,
            CLASS_ENROLL.LAST_ENRL_DT_STMP,
            CLASS_ENROLL.LAST_ENRL_TM_STMP,
            CLASS_ENROLL.LAST_DROP_DT_STMP,
            CLASS_ENROLL.LAST_DROP_TM_STMP,
            CLASS_ENROLL.APPROVAL_DATE
  from UM_F_STDNT_TERM A
  join PS_D_PERSON P
    on A.PERSON_SID = P.PERSON_SID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join UM_F_CLASS_ENRLMT CLASS_ENROLL
    on A.PERSON_SID = CLASS_ENROLL.PERSON_SID
   and A.TERM_SID = CLASS_ENROLL.TERM_SID
   and A.SRC_SYS_ID = CLASS_ENROLL.SRC_SYS_ID
  left outer join E4
    on A.INSTITUTION_CD = E4.INSTITUTION_CD
   and A.ACAD_CAR_CD = E4.ACAD_CAR_CD
   and A.TERM_CD = E4.TERM_CD
   and A.PERSON_ID = E4.PERSON_ID
   and A.SRC_SYS_ID = E4.SRC_SYS_ID)
select /*+ INLINE PARALLEL(8) */
          TERM.TERM_SID,
          TERM.PERSON_SID,
          TERM.CLASS_NUM,
          TERM.SRC_SYS_ID,
          TERM.INSTITUTION_CD,
          TERM.ACAD_CAR_CD,
          TERM.TERM_CD,
          TERM.SESSION_CD,
          TERM.PERSON_ID,
          TERM.INSTITUTION_SID,
          TERM.ACAD_CAR_SID,
          TERM.SESSION_SID,
          TERM.CLASS_SID,
       nvl(T.TERM_SID,2147483646) ENRLMT_MAX_TERM_SID,              -- Mar 2018
       nvl(T2.TERM_SID,2147483646) ENRLMT_MIN_TERM_SID,             -- June 2018
--       nvl(T3.TERM_SID,2147483646) ENRLMT_MIN_PERSON_TERM_SID,      -- June 2018
       nvl((select min(TERM_SID) from PS_D_TERM T3 where TERM.INSTITUTION_CD = T3.INSTITUTION_CD and TERM.ENRLMT_MIN_PERSON_TERM_CD = T3.TERM_CD and TERM.SRC_SYS_ID = T3.SRC_SYS_ID), 2147483646) ENRLMT_MIN_PERSON_TERM_SID,          -- June 2018
       nvl(T4.TERM_SID,2147483646) ENRLMT_PREV_TERM_SID,            -- June 2018
          TERM.ENRLMT_STAT_SID,
          TERM.ENRLMT_REAS_SID,
          TERM.ENRL_ACN_LAST_SID,
          TERM.ENRL_ACN_RSN_LAST_SID,
          TERM.GRADE_SID,
          TERM.GRADE_INPUT_SID,
          TERM.MID_TERM_GRADE_SID,
          TERM.REPEAT_SID,
          ASSOCIATED_CLASS,
          TO_CHAR (NVL (CLASS_NUM, 0)) CLASS_CD,
          CLASS_PRMSN_NBR,
          EARN_CREDIT_FLG,
          ENRL_ACTN_PRC_LAST,
       nvl(X1.XLATSHORTNAME,' ') ENRL_ACTN_PRC_LAST_SD,
       nvl(X1.XLATLONGNAME,' ') ENRL_ACTN_PRC_LAST_LD,
          ENRL_STATUS_DT,
          ENRL_ADD_DT,
          ENRL_DROP_DT,
       nvl(ENRLMT_MAX_TERM_CD,'-') ENRLMT_MAX_TERM_CD,                        -- Mar 2018
       nvl(ENRLMT_MIN_TERM_CD,'-') ENRLMT_MIN_TERM_CD,                        -- June 2018
       nvl(ENRLMT_MIN_PERSON_TERM_CD,'-') ENRLMT_MIN_PERSON_TERM_CD,          -- June 2018
       nvl(ENRLMT_PREV_TERM_CD,'-') ENRLMT_PREV_TERM_CD,                      -- June 2018
          ENRL_REQ_SOURCE,
       nvl(X2.XLATSHORTNAME,' ') ENRL_REQ_SOURCE_SD,
       nvl(X2.XLATLONGNAME,' ') ENRL_REQ_SOURCE_LD,
          GRADE_DT,
          GRADE_CATEGORY,
          GRADE_BASIS_CD,                                                     -- Added Mar 2014
       nvl(X3.XLATSHORTNAME,' ') GRADE_BASIS_SD,
       nvl(X3.XLATLONGNAME,' ') GRADE_BASIS_LD,
          GRADE_BASIS_DT,
          GRADE_BASIS_OVRD_FLG,
          INCLUDE_IN_GPA_FLG,
          LAST_UPD_ENREQ_SRC,
       nvl(X4.XLATSHORTNAME,' ') LAST_UPD_ENREQ_SRC_SD,
       nvl(X4.XLATLONGNAME,' ') LAST_UPD_ENREQ_SRC_LD,
          MANDATORY_GRD_BAS_FLG,
          NOTIFY_STDNT_CHNG,
       nvl(X5.XLATSHORTNAME,' ') NOTIFY_STDNT_CHNG_SD,
       nvl(X5.XLATLONGNAME,' ') NOTIFY_STDNT_CHNG_LD,
          REPEAT_DT,
          REPEAT_FLG,
          RSRV_CAP_NBR,
          STDNT_POSITIN,
          TSCRPT_NOTE_ID,           -- Aug 2016
          TSCRPT_NOTE_DESCR,        -- Aug 2016
          TSCRPT_NOTE_EXISTS,       -- Aug 2016
          TSCRPT_NOTE254,           -- Aug 2016
          UM_STD_COMPL_CRSE_FLG,    -- May 2016
          UM_STD_NEVER_ATTND_FLG,   -- May 2016
          UM_STD_LST_DT_ATTD,       -- May 2016
          UNITS_ATTEMPTED_CD,
       nvl(X6.XLATSHORTNAME,' ') UNITS_ATTEMPTED_SD,
       nvl(X6.XLATLONGNAME,' ') UNITS_ATTEMPTED_LD,
          VALID_ATTEMPT_FLG,
          AUDIT_CNT,                                    -- Added APR 2015
          BILLING_UNIT,
          CE_CREDITS,
          CE_FTE,
          DAY_CREDITS,
          DAY_FTE,
          CRSE_CNT,
          DROP_CNT,
          ENROLL_CNT,
          ERN_UNIT,
          GRADE_PTS,
          GRADE_PTS_FA,
          GRADE_PTS_PER_UNIT,
          IFTE_CNT,
          ONLINE_CNT,                                                               -- Added APR 2015
          (case when ONLINE_CNT > 0 then TAKEN_UNIT else 0 end) ONLINE_CREDITS,     -- Added JUN 2015
          (case when ONLINE_CNT > 0 then CE_CREDITS else 0 end) CE_ONLINE_CREDITS,  -- Added OCT 2020
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
          'S' DATA_ORIGIN,
          SYSDATE CREATED_EW_DTTM,
          SYSDATE LASTUPD_EW_DTTM,
          TERM.APPROVAL_DATE  --Added OCT 2021
from TERM
  left outer join PS_D_TERM T
    on TERM.INSTITUTION_CD = T.INSTITUTION_CD
   and TERM.ACAD_CAR_CD = T.ACAD_CAR_CD
   and TERM.ENRLMT_MAX_TERM_CD = T.TERM_CD
   and TERM.SRC_SYS_ID = T.SRC_SYS_ID
  left outer join PS_D_TERM T2
    on TERM.INSTITUTION_CD = T2.INSTITUTION_CD
   and TERM.ACAD_CAR_CD = T2.ACAD_CAR_CD
   and TERM.ENRLMT_MIN_TERM_CD = T2.TERM_CD
   and TERM.SRC_SYS_ID = T2.SRC_SYS_ID
--  left outer join PS_D_TERM T3
--    on TERM.INSTITUTION_CD = T3.INSTITUTION_CD
--   and TERM.ACAD_CAR_CD = T3.ACAD_CAR_CD
--   and TERM.ENRLMT_MIN_PERSON_TERM_CD = T3.TERM_CD
--   and TERM.SRC_SYS_ID = T3.SRC_SYS_ID
  left outer join PS_D_TERM T4
    on TERM.INSTITUTION_CD = T4.INSTITUTION_CD
   and TERM.ACAD_CAR_CD = T4.ACAD_CAR_CD
   and TERM.ENRLMT_PREV_TERM_CD = T4.TERM_CD
   and TERM.SRC_SYS_ID = T4.SRC_SYS_ID
  left outer join X X1
    on TERM.ENRL_ACTN_PRC_LAST = X1.FIELDVALUE
   and TERM.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'ENRL_ACTN_PRC_LAST'
   and X1.X_ORDER = 1
  left outer join X X2
    on TERM.ENRL_REQ_SOURCE = X2.FIELDVALUE
   and TERM.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'ENRL_REQ_SOURCE'
   and X2.X_ORDER = 1
  left outer join X X3
    on TERM.GRADE_BASIS_CD = X3.FIELDVALUE
   and TERM.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'GRADING_BASIS'
   and X3.X_ORDER = 1
  left outer join X X4
    on TERM.LAST_UPD_ENREQ_SRC = X4.FIELDVALUE
   and TERM.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'LAST_UPD_ENREQ_SRC'
   and X4.X_ORDER = 1
  left outer join X X5
    on TERM.NOTIFY_STDNT_CHNG = X5.FIELDVALUE
   and TERM.SRC_SYS_ID = X5.SRC_SYS_ID
   and X5.FIELDNAME = 'NOTIFY_STDNT_CHNG'
   and X5.X_ORDER = 1
  left outer join X X6
    on TERM.UNITS_ATTEMPTED_CD = X6.FIELDVALUE
   and TERM.SRC_SYS_ID = X6.SRC_SYS_ID
   and X6.FIELDNAME = 'UNITS_ATTEMPTED'
   and X6.X_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

commit;

insert into CSMRT_OWNER.UM_F_STDNT_ENRL
   (TERM_SID, PERSON_SID, CLASS_NUM, SRC_SYS_ID, INSTITUTION_CD,
    ACAD_CAR_CD, TERM_CD, SESSION_CD, PERSON_ID, INSTITUTION_SID,
    ACAD_CAR_SID, SESSION_SID, CLASS_SID, ENRLMT_MAX_TERM_SID, ENRLMT_MIN_TERM_SID, ENRLMT_MIN_PERSON_TERM_SID, ENRLMT_PREV_TERM_SID, ENRLMT_STAT_SID, ENRLMT_REAS_SID,
    ENRL_ACN_LAST_SID, ENRL_ACN_RSN_LAST_SID, GRADE_SID, GRADE_INPUT_SID, MID_TERM_GRADE_SID, REPEAT_SID)
values
   (2147483646, 2147483646, 0, 'CS90', '-',
    '-', '-', '-', '-', 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646, 2147483646);

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ENRL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ENRL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ENRL enable constraint PK_UM_F_STDNT_ENRL';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ENRL');

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

END UM_F_STDNT_ENRL_P;
/

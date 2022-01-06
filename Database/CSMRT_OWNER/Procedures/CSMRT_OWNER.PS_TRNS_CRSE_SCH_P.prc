CREATE OR REPLACE PROCEDURE             PS_TRNS_CRSE_SCH_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNS_CRSE_SCH from PeopleSoft table PS_TRNS_CRSE_SCH.
--
-- V01  SMT-xxxx 9/28/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TRNS_CRSE_SCH';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNS_CRSE_SCH@SASOURCE S)
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_TRNS_CRSE_SCH';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_TRNS_CRSE_SCH
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(TRNSFR_SRC_TYPE),'-') TRNSFR_SRC_TYPE, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(LS_SCHOOL_TYPE),'-') LS_SCHOOL_TYPE, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(trim(SRC_CAREER),'-') SRC_CAREER, 
    nvl(trim(SRC_INSTITUTION),'-') SRC_INSTITUTION, 
    replace(nvl(trim(SRC_ORG_NAME),'-'), '  ', ' ') SRC_ORG_NAME, 
    nvl(trim(APPLY_AGREEMENT_FL),'-') APPLY_AGREEMENT_FL, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(trim(MODEL_STATUS),'-') MODEL_STATUS, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(TRF_TAKEN_GPA,0) TRF_TAKEN_GPA, 
    nvl(TRF_TAKEN_NOGPA,0) TRF_TAKEN_NOGPA, 
    nvl(TRF_PASSED_GPA,0) TRF_PASSED_GPA, 
    nvl(TRF_PASSED_NOGPA,0) TRF_PASSED_NOGPA, 
    nvl(TRF_GRADE_POINTS,0) TRF_GRADE_POINTS, 
    nvl(TRF_GPA,0) TRF_GPA, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_TRNS_CRSE_SCH@SASOURCE S 
where EMPLID between '00000000' and '99999999'
 and length(EMPLID) = 8; 

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TRNS_CRSE_SCH';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TRNS_CRSE_SCH';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TRNS_CRSE_SCH T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(TRNSFR_SRC_TYPE),'-') TRNSFR_SRC_TYPE, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(LS_SCHOOL_TYPE),'-') LS_SCHOOL_TYPE, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(trim(SRC_CAREER),'-') SRC_CAREER, 
    nvl(trim(SRC_INSTITUTION),'-') SRC_INSTITUTION, 
    nvl(trim(SRC_ORG_NAME),'-') SRC_ORG_NAME, 
    nvl(trim(APPLY_AGREEMENT_FL),'-') APPLY_AGREEMENT_FL, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(trim(MODEL_STATUS),'-') MODEL_STATUS, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(TRF_TAKEN_GPA,0) TRF_TAKEN_GPA, 
    nvl(TRF_TAKEN_NOGPA,0) TRF_TAKEN_NOGPA, 
    nvl(TRF_PASSED_GPA,0) TRF_PASSED_GPA, 
    nvl(TRF_PASSED_NOGPA,0) TRF_PASSED_NOGPA, 
    nvl(TRF_GRADE_POINTS,0) TRF_GRADE_POINTS, 
    nvl(TRF_GPA,0) TRF_GPA, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    COMMENTS COMMENTS
from CSSTG_OWNER.PS_T_TRNS_CRSE_SCH S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_SCH') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.MODEL_NBR = S.MODEL_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ACAD_PROG = S.ACAD_PROG,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.TRNSFR_SRC_TYPE = S.TRNSFR_SRC_TYPE,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.LS_SCHOOL_TYPE = S.LS_SCHOOL_TYPE,
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE,
    T.SRC_CAREER = S.SRC_CAREER,
    T.SRC_INSTITUTION = S.SRC_INSTITUTION,
    T.SRC_ORG_NAME = S.SRC_ORG_NAME,
    T.APPLY_AGREEMENT_FL = S.APPLY_AGREEMENT_FL,
    T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
    T.MODEL_STATUS = S.MODEL_STATUS,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.UNT_TRNSFR = S.UNT_TRNSFR,
    T.TRF_TAKEN_GPA = S.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA = S.TRF_TAKEN_NOGPA,
    T.TRF_PASSED_GPA = S.TRF_PASSED_GPA,
    T.TRF_PASSED_NOGPA = S.TRF_PASSED_NOGPA,
    T.TRF_GRADE_POINTS = S.TRF_GRADE_POINTS,
    T.TRF_GPA = S.TRF_GPA,
    T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ACAD_PROG <> S.ACAD_PROG or 
    T.ACAD_PLAN <> S.ACAD_PLAN or 
    T.TRNSFR_SRC_TYPE <> S.TRNSFR_SRC_TYPE or 
    T.EXT_ORG_ID <> S.EXT_ORG_ID or 
    T.LS_SCHOOL_TYPE <> S.LS_SCHOOL_TYPE or 
    T.LS_DATA_SOURCE <> S.LS_DATA_SOURCE or 
    T.SRC_CAREER <> S.SRC_CAREER or 
    T.SRC_INSTITUTION <> S.SRC_INSTITUTION or 
    T.SRC_ORG_NAME <> S.SRC_ORG_NAME or 
    T.APPLY_AGREEMENT_FL <> S.APPLY_AGREEMENT_FL or 
    T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or 
    T.MODEL_STATUS <> S.MODEL_STATUS or 
    T.UNT_TAKEN <> S.UNT_TAKEN or 
    T.UNT_TRNSFR <> S.UNT_TRNSFR or 
    T.TRF_TAKEN_GPA <> S.TRF_TAKEN_GPA or 
    T.TRF_TAKEN_NOGPA <> S.TRF_TAKEN_NOGPA or 
    T.TRF_PASSED_GPA <> S.TRF_PASSED_GPA or 
    T.TRF_PASSED_NOGPA <> S.TRF_PASSED_NOGPA or 
    T.TRF_GRADE_POINTS <> S.TRF_GRADE_POINTS or 
    T.TRF_GPA <> S.TRF_GPA or 
    T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.MODEL_NBR,
    T.SRC_SYS_ID, 
    T.ACAD_PROG,
    T.ACAD_PLAN,
    T.TRNSFR_SRC_TYPE,
    T.EXT_ORG_ID, 
    T.LS_SCHOOL_TYPE, 
    T.LS_DATA_SOURCE, 
    T.SRC_CAREER, 
    T.SRC_INSTITUTION,
    T.SRC_ORG_NAME, 
    T.APPLY_AGREEMENT_FL, 
    T.TRANSCRIPT_LEVEL, 
    T.MODEL_STATUS, 
    T.UNT_TAKEN,
    T.UNT_TRNSFR, 
    T.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA,
    T.TRF_PASSED_GPA, 
    T.TRF_PASSED_NOGPA, 
    T.TRF_GRADE_POINTS, 
    T.TRF_GPA,
    T.INCLUDE_IN_GPA, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.MODEL_NBR,
    'CS90', 
    S.ACAD_PROG,
    S.ACAD_PLAN,
    S.TRNSFR_SRC_TYPE,
    S.EXT_ORG_ID, 
    S.LS_SCHOOL_TYPE, 
    S.LS_DATA_SOURCE, 
    S.SRC_CAREER, 
    S.SRC_INSTITUTION,
    S.SRC_ORG_NAME, 
    S.APPLY_AGREEMENT_FL, 
    S.TRANSCRIPT_LEVEL, 
    S.MODEL_STATUS, 
    S.UNT_TAKEN,
    S.UNT_TRNSFR, 
    S.TRF_TAKEN_GPA,
    S.TRF_TAKEN_NOGPA,
    S.TRF_PASSED_GPA, 
    S.TRF_PASSED_NOGPA, 
    S.TRF_GRADE_POINTS, 
    S.TRF_GPA,
    S.INCLUDE_IN_GPA, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_TRNS_CRSE_SCH rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_CRSE_SCH',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_CRSE_SCH';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_CRSE_SCH';
update CSSTG_OWNER.PS_TRNS_CRSE_SCH T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR
   from CSSTG_OWNER.PS_TRNS_CRSE_SCH T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_SCH') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR
   from SYSADM.PS_TRNS_CRSE_SCH@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_SCH') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.MODEL_NBR = S.MODEL_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNS_CRSE_SCH rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_CRSE_SCH',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNS_CRSE_SCH'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END PS_TRNS_CRSE_SCH_P;
/

DROP PROCEDURE CSMRT_OWNER.AM_PS_TRNS_TEST_MODEL_P
/

--
-- AM_PS_TRNS_TEST_MODEL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_TRNS_TEST_MODEL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNS_TEST_MODEL from PeopleSoft table PS_TRNS_TEST_MODEL.
--
-- V01  SMT-xxxx 9/29/2017,    James Doucette
--                             Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TRNS_TEST_MODEL';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNS_TEST_MODEL@AMSOURCE S)
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_TRNS_TEST_MODEL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_TRNS_TEST_MODEL
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(MODEL_STATUS),'-') MODEL_STATUS, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(TRF_PASSED_GPA,0) TRF_PASSED_GPA, 
    nvl(TRF_PASSED_NOGPA,0) TRF_PASSED_NOGPA, 
    nvl(TRF_TAKEN_GPA,0) TRF_TAKEN_GPA, 
    nvl(TRF_TAKEN_NOGPA,0) TRF_TAKEN_NOGPA, 
    nvl(TRF_GRADE_POINTS,0) TRF_GRADE_POINTS, 
    nvl(TRF_GPA,0) TRF_GPA, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_TRNS_TEST_MODEL@AMSOURCE S 
where EMPLID between '00000000' and '99999999'
 and length(EMPLID) = 8; 

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TRNS_TEST_MODEL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TRNS_TEST_MODEL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TRNS_TEST_MODEL T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(MODEL_STATUS),'-') MODEL_STATUS, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(TRF_PASSED_GPA,0) TRF_PASSED_GPA, 
    nvl(TRF_PASSED_NOGPA,0) TRF_PASSED_NOGPA, 
    nvl(TRF_TAKEN_GPA,0) TRF_TAKEN_GPA, 
    nvl(TRF_TAKEN_NOGPA,0) TRF_TAKEN_NOGPA, 
    nvl(TRF_GRADE_POINTS,0) TRF_GRADE_POINTS, 
    nvl(TRF_GPA,0) TRF_GPA, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    COMMENTS
from AMSTG_OWNER.PS_T_TRNS_TEST_MODEL S 
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_MODEL')
 and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 
 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.MODEL_NBR = S.MODEL_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.MODEL_STATUS = S.MODEL_STATUS,
    T.ACAD_PROG = S.ACAD_PROG,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
    T.UNT_TRNSFR = S.UNT_TRNSFR,
    T.TRF_PASSED_GPA = S.TRF_PASSED_GPA,
    T.TRF_PASSED_NOGPA = S.TRF_PASSED_NOGPA,
    T.TRF_TAKEN_GPA = S.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA = S.TRF_TAKEN_NOGPA,
    T.TRF_GRADE_POINTS = S.TRF_GRADE_POINTS,
    T.TRF_GPA = S.TRF_GPA,
    T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.MODEL_STATUS <> S.MODEL_STATUS or 
    T.ACAD_PROG <> S.ACAD_PROG or 
    T.ACAD_PLAN <> S.ACAD_PLAN or 
    T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or 
    T.UNT_TRNSFR <> S.UNT_TRNSFR or 
    T.TRF_PASSED_GPA <> S.TRF_PASSED_GPA or 
    T.TRF_PASSED_NOGPA <> S.TRF_PASSED_NOGPA or 
    T.TRF_TAKEN_GPA <> S.TRF_TAKEN_GPA or 
    T.TRF_TAKEN_NOGPA <> S.TRF_TAKEN_NOGPA or 
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
    T.MODEL_STATUS, 
    T.ACAD_PROG,
    T.ACAD_PLAN,
    T.TRANSCRIPT_LEVEL, 
    T.UNT_TRNSFR, 
    T.TRF_PASSED_GPA, 
    T.TRF_PASSED_NOGPA, 
    T.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA,
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
    S.MODEL_STATUS, 
    S.ACAD_PROG,
    S.ACAD_PLAN,
    S.TRANSCRIPT_LEVEL, 
    S.UNT_TRNSFR, 
    S.TRF_PASSED_GPA, 
    S.TRF_PASSED_NOGPA, 
    S.TRF_TAKEN_GPA,
    S.TRF_TAKEN_NOGPA,
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


strMessage01    := '# of PS_TRNS_TEST_MODEL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_MODEL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TRNS_TEST_MODEL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TRNS_TEST_MODEL';
update AMSTG_OWNER.PS_TRNS_TEST_MODEL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR
   from AMSTG_OWNER.PS_TRNS_TEST_MODEL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_MODEL') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR
   from SYSADM.PS_TRNS_TEST_MODEL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_MODEL') = 'Y'
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

strMessage01    := '# of PS_TRNS_TEST_MODEL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_MODEL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNS_TEST_MODEL'
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

END AM_PS_TRNS_TEST_MODEL_P;
/

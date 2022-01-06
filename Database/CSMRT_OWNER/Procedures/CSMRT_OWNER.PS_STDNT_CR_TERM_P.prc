CREATE OR REPLACE PROCEDURE             "PS_STDNT_CR_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_CR_TERM from PeopleSoft table PS_STDNT_CR_TERM.
--
-- V01  SMT-xxxx 04/11/2017,    Jim Doucette
--                              Converted from PS_STDNT_CR_TERM.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_CR_TERM';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_STDNT_CR_TERM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_CR_TERM@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_CR_TERM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_CR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_CR_TERM';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_CR_TERM T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(STRM),'-') STRM, 
    nvl(FA_TERM_WEEKS,0) FA_TERM_WEEKS, 
    nvl(trim(AWARD_PERIOD),'-') AWARD_PERIOD
  from SYSADM.PS_STDNT_CR_TERM@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_CR_TERM')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STRM = S.STRM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.FA_TERM_WEEKS = S.FA_TERM_WEEKS,
    T.AWARD_PERIOD = S.AWARD_PERIOD,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.FA_TERM_WEEKS <> S.FA_TERM_WEEKS or 
    T.AWARD_PERIOD <> S.AWARD_PERIOD or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.ACAD_CAREER,
    T.STRM, 
    T.SRC_SYS_ID, 
    T.FA_TERM_WEEKS,
    T.AWARD_PERIOD, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.INSTITUTION,
    S.AID_YEAR, 
    S.ACAD_CAREER,
    S.STRM, 
    'CS90', 
    S.FA_TERM_WEEKS,
    S.AWARD_PERIOD, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_CR_TERM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_CR_TERM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_CR_TERM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_CR_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_CR_TERM';
update CSSTG_OWNER.PS_STDNT_CR_TERM T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM
   from CSSTG_OWNER.PS_STDNT_CR_TERM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_CR_TERM') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM
   from SYSADM.PS_STDNT_CR_TERM@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_CR_TERM') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STRM = S.STRM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_CR_TERM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_CR_TERM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_CR_TERM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_STDNT_CR_TERM_P;
/

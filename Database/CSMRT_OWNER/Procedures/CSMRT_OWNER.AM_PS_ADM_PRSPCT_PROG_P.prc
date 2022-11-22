DROP PROCEDURE CSMRT_OWNER.AM_PS_ADM_PRSPCT_PROG_P
/

--
-- AM_PS_ADM_PRSPCT_PROG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ADM_PRSPCT_PROG_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_PRSPCT_PROG from PeopleSoft table PS_ADM_PRSPCT_PROG.
--
 --V01  SMT-xxxx 10/02/2017,    James Doucette
--                              Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ADM_PRSPCT_PROG';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_ADM_PRSPCT_PROG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_PRSPCT_PROG@AMSOURCE S)
 where TABLE_NAME = 'PS_ADM_PRSPCT_PROG'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ADM_PRSPCT_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ADM_PRSPCT_PROG';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ADM_PRSPCT_PROG T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ADM_RECR_CTR),'-') ADM_RECR_CTR, 
    nvl(trim(RECRUITING_STATUS),'-') RECRUITING_STATUS, 
    RECR_STATUS_DT,
    nvl(trim(CAMPUS),'-') CAMPUS, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR
from SYSADM.PS_ADM_PRSPCT_PROG@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_PROG')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_PROG = S.ACAD_PROG and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ADM_RECR_CTR = S.ADM_RECR_CTR,
    T.RECRUITING_STATUS = S.RECRUITING_STATUS,
    T.RECR_STATUS_DT = S.RECR_STATUS_DT,
    T.CAMPUS = S.CAMPUS,
    T.ADM_APPL_NBR = S.ADM_APPL_NBR,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ADM_RECR_CTR <> S.ADM_RECR_CTR or 
    T.RECRUITING_STATUS <> S.RECRUITING_STATUS or 
    nvl(trim(T.RECR_STATUS_DT),0) <> nvl(trim(S.RECR_STATUS_DT),0) or 
    T.CAMPUS <> S.CAMPUS or 
    T.ADM_APPL_NBR <> S.ADM_APPL_NBR or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.ACAD_PROG,
    T.SRC_SYS_ID, 
    T.ADM_RECR_CTR, 
    T.RECRUITING_STATUS,
    T.RECR_STATUS_DT, 
    T.CAMPUS, 
    T.ADM_APPL_NBR, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.ACAD_PROG,
    'CS90', 
    S.ADM_RECR_CTR, 
    S.RECRUITING_STATUS,
    S.RECR_STATUS_DT, 
    S.CAMPUS, 
    S.ADM_APPL_NBR, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_PRSPCT_PROG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_PRSPCT_PROG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_PRSPCT_PROG';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ADM_PRSPCT_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ADM_PRSPCT_PROG';
update AMSTG_OWNER.PS_ADM_PRSPCT_PROG T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, ACAD_PROG
   from AMSTG_OWNER.PS_ADM_PRSPCT_PROG T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_PROG') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, ACAD_PROG
   from SYSADM.PS_ADM_PRSPCT_PROG@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_PROG') = 'Y' 
   ) S
 where T.EMPLID= S.EMPLID
    AND T.ACAD_CAREER = S.ACAD_CAREER
    AND T.INSTITUTION = S.INSTITUTION
    AND T.ACAD_PROG = S.ACAD_PROG
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_PRSPCT_PROG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_PRSPCT_PROG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_PRSPCT_PROG'
;

strSqlCommand := 'commit';
commit;


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

END AM_PS_ADM_PRSPCT_PROG_P;
/

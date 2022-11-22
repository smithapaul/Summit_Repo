DROP PROCEDURE CSMRT_OWNER.PS_UM_STD_LST_ATND_P
/

--
-- PS_UM_STD_LST_ATND_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_STD_LST_ATND_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_STD_LST_ATND'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_UM_STD_LST_ATND', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_STD_LST_ATND'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_STD_LST_ATND from PeopleSoft table PS_UM_STD_LST_ATND.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_UM_STD_LST_ATND.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_STD_LST_ATND';
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
 where TABLE_NAME = 'PS_UM_STD_LST_ATND'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_STD_LST_ATND@SASOURCE S)
 where TABLE_NAME = 'PS_UM_STD_LST_ATND'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_STD_LST_ATND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_STD_LST_ATND';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_STD_LST_ATND T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(STRM),'-') STRM,
    nvl(CLASS_NBR,0) CLASS_NBR,
    nvl(trim(UM_STD_COMPL_CRSE),'-') UM_STD_COMPL_CRSE,
    nvl(trim(UM_STD_NEVER_ATTND),'-') UM_STD_NEVER_ATTND,
    to_date(to_char(UM_STD_LST_DT_ATTD,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_STD_LST_DT_ATTD,
    nvl(trim(UM_UPDT_ID),'-') UM_UPDT_ID,
    to_date(to_char(LAST_UPDATE_DTTM,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_UPDATE_DTTM
  from SYSADM.PS_UM_STD_LST_ATND@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STD_LST_ATND')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
    T.EMPLID = S.EMPLID and
    T.ACAD_CAREER = S.ACAD_CAREER and
    T.INSTITUTION = S.INSTITUTION and
    T.STRM = S.STRM and
    T.CLASS_NBR = S.CLASS_NBR and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.UM_STD_COMPL_CRSE = S.UM_STD_COMPL_CRSE,
    T.UM_STD_NEVER_ATTND = S.UM_STD_NEVER_ATTND,
    T.UM_STD_LST_DT_ATTD = S.UM_STD_LST_DT_ATTD,
    T.UM_UPDT_ID = S.UM_UPDT_ID,
    T.LAST_UPDATE_DTTM = S.LAST_UPDATE_DTTM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.UM_STD_COMPL_CRSE <> S.UM_STD_COMPL_CRSE or
    T.UM_STD_NEVER_ATTND <> S.UM_STD_NEVER_ATTND or
    nvl(trim(T.UM_STD_LST_DT_ATTD),0) <> nvl(trim(S.UM_STD_LST_DT_ATTD),0) or
    T.UM_UPDT_ID <> S.UM_UPDT_ID or
    nvl(trim(T.LAST_UPDATE_DTTM),0) <> nvl(trim(S.LAST_UPDATE_DTTM),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.EMPLID,
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.STRM,
    T.CLASS_NBR,
    T.SRC_SYS_ID,
    T.UM_STD_COMPL_CRSE,
    T.UM_STD_NEVER_ATTND,
    T.UM_STD_LST_DT_ATTD,
    T.UM_UPDT_ID,
    T.LAST_UPDATE_DTTM,
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
    S.STRM,
    S.CLASS_NBR,
    'CS90',
    S.UM_STD_COMPL_CRSE,
    S.UM_STD_NEVER_ATTND,
    S.UM_STD_LST_DT_ATTD,
    S.UM_UPDT_ID,
    S.LAST_UPDATE_DTTM,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STD_LST_ATND rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STD_LST_ATND',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_STD_LST_ATND';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_STD_LST_ATND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_STD_LST_ATND';
update CSSTG_OWNER.PS_UM_STD_LST_ATND T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR
   from CSSTG_OWNER.PS_UM_STD_LST_ATND T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STD_LST_ATND') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR
   from SYSADM.PS_UM_STD_LST_ATND@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STD_LST_ATND') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.STRM = S.STRM 
   and T.CLASS_NBR = S.CLASS_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STD_LST_ATND rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STD_LST_ATND',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_STD_LST_ATND'
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

END PS_UM_STD_LST_ATND_P;
/

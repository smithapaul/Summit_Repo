CREATE OR REPLACE PROCEDURE             PS_STDNT_EQUTN_VAR_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_EQUTN_VAR from PeopleSoft table PS_STDNT_EQUTN_VAR.
--
-- V01  SMT-xxxx 03/29/2017,    George Adams
--                              Converted from PS_STDNT_EQUTN_VAR.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_EQUTN_VAR';
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
 where TABLE_NAME = 'PS_STDNT_EQUTN_VAR'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_EQUTN_VAR@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_EQUTN_VAR'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_EQUTN_VAR';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_EQUTN_VAR T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(BILLING_CAREER),'-') BILLING_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(VARIABLE_CHAR1),'-') VARIABLE_CHAR1, 
    nvl(trim(VARIABLE_CHAR2),'-') VARIABLE_CHAR2, 
    nvl(trim(VARIABLE_CHAR3),'-') VARIABLE_CHAR3, 
    nvl(trim(VARIABLE_CHAR4),'-') VARIABLE_CHAR4, 
    nvl(trim(VARIABLE_CHAR5),'-') VARIABLE_CHAR5, 
    nvl(trim(VARIABLE_CHAR6),'-') VARIABLE_CHAR6, 
    nvl(trim(VARIABLE_CHAR7),'-') VARIABLE_CHAR7, 
    nvl(trim(VARIABLE_CHAR8),'-') VARIABLE_CHAR8, 
    nvl(trim(VARIABLE_CHAR9),'-') VARIABLE_CHAR9, 
    nvl(trim(VARIABLE_CHAR10),'-') VARIABLE_CHAR10, 
    nvl(trim(VARIABLE_FLAG1),'-') VARIABLE_FLAG1, 
    nvl(trim(VARIABLE_FLAG2),'-') VARIABLE_FLAG2, 
    nvl(trim(VARIABLE_FLAG3),'-') VARIABLE_FLAG3, 
    nvl(trim(VARIABLE_FLAG4),'-') VARIABLE_FLAG4, 
    nvl(trim(VARIABLE_FLAG5),'-') VARIABLE_FLAG5, 
    nvl(trim(VARIABLE_FLAG6),'-') VARIABLE_FLAG6, 
    nvl(trim(VARIABLE_FLAG7),'-') VARIABLE_FLAG7, 
    nvl(trim(VARIABLE_FLAG8),'-') VARIABLE_FLAG8, 
    nvl(trim(VARIABLE_FLAG9),'-') VARIABLE_FLAG9, 
    nvl(trim(VARIABLE_FLAG10),'-') VARIABLE_FLAG10, 
    nvl(VARIABLE_NUM1,0) VARIABLE_NUM1, 
    nvl(VARIABLE_NUM2,0) VARIABLE_NUM2, 
    nvl(VARIABLE_NUM3,0) VARIABLE_NUM3, 
    nvl(VARIABLE_NUM4,0) VARIABLE_NUM4, 
    nvl(VARIABLE_NUM5,0) VARIABLE_NUM5, 
    nvl(VARIABLE_NUM6,0) VARIABLE_NUM6, 
    nvl(VARIABLE_NUM7,0) VARIABLE_NUM7, 
    nvl(VARIABLE_NUM8,0) VARIABLE_NUM8, 
    nvl(VARIABLE_NUM9,0) VARIABLE_NUM9, 
    nvl(VARIABLE_NUM10,0) VARIABLE_NUM10
  from SYSADM.PS_STDNT_EQUTN_VAR@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_EQUTN_VAR')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.BILLING_CAREER = S.BILLING_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.STRM = S.STRM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.VARIABLE_CHAR1 = S.VARIABLE_CHAR1,
    T.VARIABLE_CHAR2 = S.VARIABLE_CHAR2,
    T.VARIABLE_CHAR3 = S.VARIABLE_CHAR3,
    T.VARIABLE_CHAR4 = S.VARIABLE_CHAR4,
    T.VARIABLE_CHAR5 = S.VARIABLE_CHAR5,
    T.VARIABLE_CHAR6 = S.VARIABLE_CHAR6,
    T.VARIABLE_CHAR7 = S.VARIABLE_CHAR7,
    T.VARIABLE_CHAR8 = S.VARIABLE_CHAR8,
    T.VARIABLE_CHAR9 = S.VARIABLE_CHAR9,
    T.VARIABLE_CHAR10 = S.VARIABLE_CHAR10,
    T.VARIABLE_FLAG1 = S.VARIABLE_FLAG1,
    T.VARIABLE_FLAG2 = S.VARIABLE_FLAG2,
    T.VARIABLE_FLAG3 = S.VARIABLE_FLAG3,
    T.VARIABLE_FLAG4 = S.VARIABLE_FLAG4,
    T.VARIABLE_FLAG5 = S.VARIABLE_FLAG5,
    T.VARIABLE_FLAG6 = S.VARIABLE_FLAG6,
    T.VARIABLE_FLAG7 = S.VARIABLE_FLAG7,
    T.VARIABLE_FLAG8 = S.VARIABLE_FLAG8,
    T.VARIABLE_FLAG9 = S.VARIABLE_FLAG9,
    T.VARIABLE_FLAG10 = S.VARIABLE_FLAG10,
    T.VARIABLE_NUM1 = S.VARIABLE_NUM1,
    T.VARIABLE_NUM2 = S.VARIABLE_NUM2,
    T.VARIABLE_NUM3 = S.VARIABLE_NUM3,
    T.VARIABLE_NUM4 = S.VARIABLE_NUM4,
    T.VARIABLE_NUM5 = S.VARIABLE_NUM5,
    T.VARIABLE_NUM6 = S.VARIABLE_NUM6,
    T.VARIABLE_NUM7 = S.VARIABLE_NUM7,
    T.VARIABLE_NUM8 = S.VARIABLE_NUM8,
    T.VARIABLE_NUM9 = S.VARIABLE_NUM9,
    T.VARIABLE_NUM10 = S.VARIABLE_NUM10,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.VARIABLE_CHAR1 <> S.VARIABLE_CHAR1 or 
    T.VARIABLE_CHAR2 <> S.VARIABLE_CHAR2 or 
    T.VARIABLE_CHAR3 <> S.VARIABLE_CHAR3 or 
    T.VARIABLE_CHAR4 <> S.VARIABLE_CHAR4 or 
    T.VARIABLE_CHAR5 <> S.VARIABLE_CHAR5 or 
    T.VARIABLE_CHAR6 <> S.VARIABLE_CHAR6 or 
    T.VARIABLE_CHAR7 <> S.VARIABLE_CHAR7 or 
    T.VARIABLE_CHAR8 <> S.VARIABLE_CHAR8 or 
    T.VARIABLE_CHAR9 <> S.VARIABLE_CHAR9 or 
    T.VARIABLE_CHAR10 <> S.VARIABLE_CHAR10 or 
    T.VARIABLE_FLAG1 <> S.VARIABLE_FLAG1 or 
    T.VARIABLE_FLAG2 <> S.VARIABLE_FLAG2 or 
    T.VARIABLE_FLAG3 <> S.VARIABLE_FLAG3 or 
    T.VARIABLE_FLAG4 <> S.VARIABLE_FLAG4 or 
    T.VARIABLE_FLAG5 <> S.VARIABLE_FLAG5 or 
    T.VARIABLE_FLAG6 <> S.VARIABLE_FLAG6 or 
    T.VARIABLE_FLAG7 <> S.VARIABLE_FLAG7 or 
    T.VARIABLE_FLAG8 <> S.VARIABLE_FLAG8 or 
    T.VARIABLE_FLAG9 <> S.VARIABLE_FLAG9 or 
    T.VARIABLE_FLAG10 <> S.VARIABLE_FLAG10 or 
    T.VARIABLE_NUM1 <> S.VARIABLE_NUM1 or 
    T.VARIABLE_NUM2 <> S.VARIABLE_NUM2 or 
    T.VARIABLE_NUM3 <> S.VARIABLE_NUM3 or 
    T.VARIABLE_NUM4 <> S.VARIABLE_NUM4 or 
    T.VARIABLE_NUM5 <> S.VARIABLE_NUM5 or 
    T.VARIABLE_NUM6 <> S.VARIABLE_NUM6 or 
    T.VARIABLE_NUM7 <> S.VARIABLE_NUM7 or 
    T.VARIABLE_NUM8 <> S.VARIABLE_NUM8 or 
    T.VARIABLE_NUM9 <> S.VARIABLE_NUM9 or 
    T.VARIABLE_NUM10 <> S.VARIABLE_NUM10 or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.BILLING_CAREER, 
    T.INSTITUTION,
    T.STRM, 
    T.SRC_SYS_ID, 
    T.VARIABLE_CHAR1, 
    T.VARIABLE_CHAR2, 
    T.VARIABLE_CHAR3, 
    T.VARIABLE_CHAR4, 
    T.VARIABLE_CHAR5, 
    T.VARIABLE_CHAR6, 
    T.VARIABLE_CHAR7, 
    T.VARIABLE_CHAR8, 
    T.VARIABLE_CHAR9, 
    T.VARIABLE_CHAR10,
    T.VARIABLE_FLAG1, 
    T.VARIABLE_FLAG2, 
    T.VARIABLE_FLAG3, 
    T.VARIABLE_FLAG4, 
    T.VARIABLE_FLAG5, 
    T.VARIABLE_FLAG6, 
    T.VARIABLE_FLAG7, 
    T.VARIABLE_FLAG8, 
    T.VARIABLE_FLAG9, 
    T.VARIABLE_FLAG10,
    T.VARIABLE_NUM1,
    T.VARIABLE_NUM2,
    T.VARIABLE_NUM3,
    T.VARIABLE_NUM4,
    T.VARIABLE_NUM5,
    T.VARIABLE_NUM6,
    T.VARIABLE_NUM7,
    T.VARIABLE_NUM8,
    T.VARIABLE_NUM9,
    T.VARIABLE_NUM10, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.BILLING_CAREER, 
    S.INSTITUTION,
    S.STRM, 
    'CS90', 
    S.VARIABLE_CHAR1, 
    S.VARIABLE_CHAR2, 
    S.VARIABLE_CHAR3, 
    S.VARIABLE_CHAR4, 
    S.VARIABLE_CHAR5, 
    S.VARIABLE_CHAR6, 
    S.VARIABLE_CHAR7, 
    S.VARIABLE_CHAR8, 
    S.VARIABLE_CHAR9, 
    S.VARIABLE_CHAR10,
    S.VARIABLE_FLAG1, 
    S.VARIABLE_FLAG2, 
    S.VARIABLE_FLAG3, 
    S.VARIABLE_FLAG4, 
    S.VARIABLE_FLAG5, 
    S.VARIABLE_FLAG6, 
    S.VARIABLE_FLAG7, 
    S.VARIABLE_FLAG8, 
    S.VARIABLE_FLAG9, 
    S.VARIABLE_FLAG10,
    S.VARIABLE_NUM1,
    S.VARIABLE_NUM2,
    S.VARIABLE_NUM3,
    S.VARIABLE_NUM4,
    S.VARIABLE_NUM5,
    S.VARIABLE_NUM6,
    S.VARIABLE_NUM7,
    S.VARIABLE_NUM8,
    S.VARIABLE_NUM9,
    S.VARIABLE_NUM10, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_EQUTN_VAR rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_EQUTN_VAR',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_EQUTN_VAR';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_EQUTN_VAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_EQUTN_VAR';
update CSSTG_OWNER.PS_STDNT_EQUTN_VAR T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, BILLING_CAREER, INSTITUTION, STRM
   from CSSTG_OWNER.PS_STDNT_EQUTN_VAR T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_EQUTN_VAR') = 'Y'
  minus
 select EMPLID, BILLING_CAREER, INSTITUTION, STRM
   from SYSADM.PS_STDNT_EQUTN_VAR@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_EQUTN_VAR') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.BILLING_CAREER = S.BILLING_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.STRM = S.STRM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_EQUTN_VAR rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_EQUTN_VAR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_EQUTN_VAR'
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

END PS_STDNT_EQUTN_VAR_P;
/

CREATE OR REPLACE PROCEDURE             "PS_STDNT_TEST_COMP_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_STDNT_TEST_COMP'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_STDNT_TEST_COMP', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_STDNT_TEST_COMP'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_TEST_COMP from PeopleSoft table PS_STDNT_TEST_COMP.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_STDNT_TEST_COMP.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_TEST_COMP';
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
 where TABLE_NAME = 'PS_STDNT_TEST_COMP'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_TEST_COMP@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_TEST_COMP'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_TEST_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_TEST_COMP';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_TEST_COMP T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(TEST_ID),'-') TEST_ID,
    nvl(trim(TEST_COMPONENT),'-') TEST_COMPONENT,
    to_date(to_char(case when TEST_DT < '01-JAN-1800' then to_date('01-JAN-1800') else TEST_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') TEST_DT,
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE,
    nvl(SCORE,0) SCORE,
    nvl(trim(SCORE_LETTER),'-') SCORE_LETTER,
    nvl(trim(EXT_ACAD_LEVEL),'-') EXT_ACAD_LEVEL,
    to_date(to_char(case when DATE_LOADED < '01-JAN-1800' then NULL else DATE_LOADED end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DATE_LOADED,
    nvl(PERCENTILE,0) PERCENTILE,
    nvl(trim(TEST_ADMIN),'-') TEST_ADMIN,
    nvl(TEST_INDEX,0) TEST_INDEX
from SYSADM.PS_STDNT_TEST_COMP@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TEST_COMP') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S
   on (
    T.EMPLID = S.EMPLID and
    T.TEST_ID = S.TEST_ID and
    T.TEST_COMPONENT = S.TEST_COMPONENT and
    T.TEST_DT = S.TEST_DT and
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SCORE = S.SCORE,
    T.SCORE_LETTER = S.SCORE_LETTER,
    T.EXT_ACAD_LEVEL = S.EXT_ACAD_LEVEL,
    T.DATE_LOADED = S.DATE_LOADED,
    T.PERCENTILE = S.PERCENTILE,
    T.TEST_ADMIN = S.TEST_ADMIN,
    T.TEST_INDEX = S.TEST_INDEX,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.SCORE <> S.SCORE or
    T.SCORE_LETTER <> S.SCORE_LETTER or
    T.EXT_ACAD_LEVEL <> S.EXT_ACAD_LEVEL or
    nvl(trim(T.DATE_LOADED),0) <> nvl(trim(S.DATE_LOADED),0) or
    T.PERCENTILE <> S.PERCENTILE or
    T.TEST_ADMIN <> S.TEST_ADMIN or
    T.TEST_INDEX <> S.TEST_INDEX or
    T.DATA_ORIGIN = 'D'
    when not matched then
    insert (
    T.EMPLID,
    T.TEST_ID,
    T.TEST_COMPONENT,
    T.TEST_DT,
    T.LS_DATA_SOURCE,
    T.SRC_SYS_ID,
    T.SCORE,
    T.SCORE_LETTER,
    T.EXT_ACAD_LEVEL,
    T.DATE_LOADED,
    T.PERCENTILE,
    T.TEST_ADMIN,
    T.TEST_INDEX,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    )
values (
    S.EMPLID,
    S.TEST_ID,
    S.TEST_COMPONENT,
    S.TEST_DT,
    S.LS_DATA_SOURCE,
    'CS90',
    S.SCORE,
    S.SCORE_LETTER,
    S.EXT_ACAD_LEVEL,
    S.DATE_LOADED,
    S.PERCENTILE,
    S.TEST_ADMIN,
    S.TEST_INDEX,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_TEST_COMP rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_TEST_COMP',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_TEST_COMP';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_TEST_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_TEST_COMP';
update CSSTG_OWNER.PS_STDNT_TEST_COMP T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, TEST_ID, TEST_COMPONENT, TEST_DT, LS_DATA_SOURCE
   from CSSTG_OWNER.PS_STDNT_TEST_COMP T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TEST_COMP') = 'Y'
  minus
 select EMPLID, TEST_ID, TEST_COMPONENT, (case when TEST_DT < '01-JAN-1800' then to_date('01-JAN-1800') else TEST_DT end) TEST_DT, LS_DATA_SOURCE
   from SYSADM.PS_STDNT_TEST_COMP@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TEST_COMP') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.TEST_ID = S.TEST_ID
   and T.TEST_COMPONENT = S.TEST_COMPONENT
   and T.TEST_DT = S.TEST_DT
   and T.LS_DATA_SOURCE = S.LS_DATA_SOURCE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_TEST_COMP rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_TEST_COMP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_TEST_COMP'
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

END PS_STDNT_TEST_COMP_P;
/

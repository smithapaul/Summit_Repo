CREATE OR REPLACE PROCEDURE             "PS_ACAD_LEVEL_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_LEVEL_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACAD_LEVEL_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_LEVEL_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_LEVEL_TBL from PeopleSoft table PS_ACAD_LEVEL_TBL.
--
 --V01  SMT-xxxx 06/06/2017,    Preethi Lodha
--                              Converted from PS_ACAD_LEVEL_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_LEVEL_TBL';
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
 where TABLE_NAME = 'PS_ACAD_LEVEL_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_LEVEL_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_LEVEL_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_LEVEL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_LEVEL_TBL';

merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_LEVEL_TBL T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(LEVEL_LOAD_RULE),'-') LEVEL_LOAD_RULE,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(UNT_CUM_TOTAL,0) UNT_CUM_TOTAL,
nvl(trim(ACADEMIC_LEVEL),'-') ACADEMIC_LEVEL,
nvl(trim(NSLDS_LOAN_YEAR),'-') NSLDS_LOAN_YEAR,
nvl(trim(DIR_LND_YR),'-') DIR_LND_YR
from SYSADM.PS_ACAD_LEVEL_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LEVEL_TBL') ) S
   on (
T.SETID = S.SETID and
T.LEVEL_LOAD_RULE = S.LEVEL_LOAD_RULE and
T.EFFDT = S.EFFDT and
T.UNT_CUM_TOTAL = S.UNT_CUM_TOTAL and
T.ACADEMIC_LEVEL = S.ACADEMIC_LEVEL and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.NSLDS_LOAN_YEAR = S.NSLDS_LOAN_YEAR,
T.DIR_LND_YR = S.DIR_LND_YR,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.NSLDS_LOAN_YEAR <> S.NSLDS_LOAN_YEAR or
T.DIR_LND_YR <> S.DIR_LND_YR or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.LEVEL_LOAD_RULE,
T.EFFDT,
T.UNT_CUM_TOTAL,
T.ACADEMIC_LEVEL,
T.SRC_SYS_ID,
T.NSLDS_LOAN_YEAR,
T.DIR_LND_YR,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.SETID,
S.LEVEL_LOAD_RULE,
S.EFFDT,
S.UNT_CUM_TOTAL,
S.ACADEMIC_LEVEL,
'CS90',
S.NSLDS_LOAN_YEAR,
S.DIR_LND_YR,
'N',
'S',
sysdate,
sysdate,
1234); 


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_LEVEL_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_LEVEL_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_LEVEL_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_LEVEL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_LEVEL_TBL';

    update CSSTG_OWNER.PS_ACAD_LEVEL_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, LEVEL_LOAD_RULE, EFFDT, UNT_CUM_TOTAL, ACADEMIC_LEVEL
   from CSSTG_OWNER.PS_ACAD_LEVEL_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LEVEL_TBL') = 'Y'
  minus
 select SETID, LEVEL_LOAD_RULE, EFFDT, UNT_CUM_TOTAL, ACADEMIC_LEVEL
   from SYSADM.PS_ACAD_LEVEL_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LEVEL_TBL') = 'Y' 
-- AND EMPLID <>'00386824'
   ) S
 where T.SETID = S.SETID
   and T.LEVEL_LOAD_RULE = S.LEVEL_LOAD_RULE
   AND T.EFFDT = S.EFFDT
    AND T.UNT_CUM_TOTAL = S.UNT_CUM_TOTAL
     AND T.ACADEMIC_LEVEL = S.ACADEMIC_LEVEL
      AND T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_LEVEL_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_LEVEL_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_LEVEL_TBL'
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

END PS_ACAD_LEVEL_TBL_P;
/

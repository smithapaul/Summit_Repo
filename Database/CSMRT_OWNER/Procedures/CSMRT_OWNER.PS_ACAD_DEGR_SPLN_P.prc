DROP PROCEDURE CSMRT_OWNER.PS_ACAD_DEGR_SPLN_P
/

--
-- PS_ACAD_DEGR_SPLN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ACAD_DEGR_SPLN_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_DEGR_SPLN'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACAD_DEGR_SPLN', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_DEGR_SPLN'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_DEGR_SPLN from PeopleSoft table PS_ACAD_DEGR_SPLN.
--
 --V01  SMT-xxxx 06/06/2017,    Preethi Lodha
--                              Converted from PS_ACAD_DEGR_SPLN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_DEGR_SPLN';
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
 where TABLE_NAME = 'PS_ACAD_DEGR_SPLN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_DEGR_SPLN@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_DEGR_SPLN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_DEGR_SPLN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_DEGR_SPLN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_DEGR_SPLN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(STDNT_DEGR),'-') STDNT_DEGR,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN,
nvl(trim(OVERRIDE_FL),'-') OVERRIDE_FL,
nvl(trim(DIPLOMA_DESCR),'-') DIPLOMA_DESCR,
nvl(trim(TRNSCR_DESCR),'-') TRNSCR_DESCR,
nvl(trim(HONORS_PREFIX),'-') HONORS_PREFIX,
nvl(trim(HONORS_SUFFIX),'-') HONORS_SUFFIX,
nvl(SUB_PLAN_SEQUENCE,0) SUB_PLAN_SEQUENCE,
nvl(trim(OPRID),'-') OPRID
from SYSADM.PS_ACAD_DEGR_SPLN@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_SPLN') ) S
   on (
T.EMPLID = S.EMPLID and
T.STDNT_DEGR = S.STDNT_DEGR and
T.ACAD_PLAN = S.ACAD_PLAN and
T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.OVERRIDE_FL = S.OVERRIDE_FL,
T.DIPLOMA_DESCR = S.DIPLOMA_DESCR,
T.TRNSCR_DESCR = S.TRNSCR_DESCR,
T.HONORS_PREFIX = S.HONORS_PREFIX,
T.HONORS_SUFFIX = S.HONORS_SUFFIX,
T.SUB_PLAN_SEQUENCE = S.SUB_PLAN_SEQUENCE,
T.OPRID = S.OPRID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.OVERRIDE_FL <> S.OVERRIDE_FL or
T.DIPLOMA_DESCR <> S.DIPLOMA_DESCR or
T.TRNSCR_DESCR <> S.TRNSCR_DESCR or
T.HONORS_PREFIX <> S.HONORS_PREFIX or
T.HONORS_SUFFIX <> S.HONORS_SUFFIX or
T.SUB_PLAN_SEQUENCE <> S.SUB_PLAN_SEQUENCE or
T.OPRID <> S.OPRID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.STDNT_DEGR,
T.ACAD_PLAN,
T.ACAD_SUB_PLAN,
T.SRC_SYS_ID,
T.OVERRIDE_FL,
T.DIPLOMA_DESCR,
T.TRNSCR_DESCR,
T.HONORS_PREFIX,
T.HONORS_SUFFIX,
T.SUB_PLAN_SEQUENCE,
T.OPRID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.STDNT_DEGR,
S.ACAD_PLAN,
S.ACAD_SUB_PLAN,
'CS90',
S.OVERRIDE_FL,
S.DIPLOMA_DESCR,
S.TRNSCR_DESCR,
S.HONORS_PREFIX,
S.HONORS_SUFFIX,
S.SUB_PLAN_SEQUENCE,
S.OPRID,
'N',
'S',
sysdate,
sysdate,
1234);
COMMIT;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_SPLN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_SPLN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_DEGR_SPLN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_DEGR_SPLN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_DEGR_SPLN';
update CSSTG_OWNER.PS_ACAD_DEGR_SPLN T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, STDNT_DEGR, ACAD_PLAN, ACAD_SUB_PLAN
   from CSSTG_OWNER.PS_ACAD_DEGR_SPLN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_SPLN') = 'Y'
  minus
 select EMPLID, STDNT_DEGR,ACAD_PLAN, ACAD_SUB_PLAN
   from SYSADM.PS_ACAD_DEGR_SPLN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_SPLN') = 'Y' 
-- AND EMPLID <>'00386824'
   ) S
 where T.EMPLID = S.EMPLID
   and T.STDNT_DEGR = S.STDNT_DEGR
   AND T.ACAD_PLAN = S.ACAD_PLAN
   AND T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_SPLN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_SPLN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_DEGR_SPLN'
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

END PS_ACAD_DEGR_SPLN_P;
/

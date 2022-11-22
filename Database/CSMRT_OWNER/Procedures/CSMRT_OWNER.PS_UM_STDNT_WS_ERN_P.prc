DROP PROCEDURE CSMRT_OWNER.PS_UM_STDNT_WS_ERN_P
/

--
-- PS_UM_STDNT_WS_ERN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_STDNT_WS_ERN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_STDNT_WS_ERN from PeopleSoft table PS_UM_STDNT_WS_ERN.
--
-- V01  SMT-xxxx 04/18/2017,    Jim Doucette
--                              Converted from PS_UM_STDNT_WS_ERN.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_STDNT_WS_ERN';
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
 where TABLE_NAME = 'PS_UM_STDNT_WS_ERN'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_STDNT_WS_ERN@SASOURCE S)
 where TABLE_NAME = 'PS_UM_STDNT_WS_ERN'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_STDNT_WS_ERN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_STDNT_WS_ERN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_STDNT_WS_ERN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID, 
nvl(trim(INSTITUTION),'-') INSTITUTION, 
nvl(trim(AID_YEAR),'-') AID_YEAR, 
CASE  WHEN TRIM(PAY_END_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE PAY_END_DT
       END  PAY_END_DT,
nvl(EMPL_RCD,0) EMPL_RCD, 
nvl(trim(ACCT_CD),'-') ACCT_CD, 
nvl(trim(DEPTID),'-') DEPTID, 
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
CASE  WHEN TRIM(LAST_RUN_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE LAST_RUN_DT
       END  LAST_RUN_DT,
CASE  WHEN TRIM(ERN_BEGIN_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE ERN_BEGIN_DT
       END  ERN_BEGIN_DT,
CASE  WHEN TRIM(ERN_END_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE ERN_END_DT
       END  ERN_END_DT,                       
HOURLY_RT HOURLY_RT,
RATE_USED RATE_USED,
REG_HRS REG_HRS,
REG_EARNS REG_EARNS,
UM_REG_EARNS UM_REG_EARNS
from SYSADM.PS_UM_STDNT_WS_ERN@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_ERN')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
T.EMPLID = S.EMPLID and 
T.INSTITUTION = S.INSTITUTION and 
T.AID_YEAR = S.AID_YEAR and 
T.PAY_END_DT = S.PAY_END_DT and 
T.EMPL_RCD = S.EMPL_RCD and 
T.ACCT_CD = S.ACCT_CD and 
T.DEPTID = S.DEPTID and 
T.ITEM_TYPE = S.ITEM_TYPE and 
T.LAST_RUN_DT = S.LAST_RUN_DT and 
T.ERN_BEGIN_DT = S.ERN_BEGIN_DT and 
T.ERN_END_DT = S.ERN_END_DT and 
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.HOURLY_RT = S.HOURLY_RT,
T.RATE_USED = S.RATE_USED,
T.REG_HRS = S.REG_HRS,
T.REG_EARNS = S.REG_EARNS,
T.UM_REG_EARNS = S.UM_REG_EARNS,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID = 1234
where 
nvl(trim(T.HOURLY_RT),0) <> nvl(trim(S.HOURLY_RT),0) or 
nvl(trim(T.RATE_USED),0) <> nvl(trim(S.RATE_USED),0) or 
nvl(trim(T.REG_HRS),0) <> nvl(trim(S.REG_HRS),0) or 
nvl(trim(T.REG_EARNS),0) <> nvl(trim(S.REG_EARNS),0) or 
nvl(trim(T.UM_REG_EARNS),0) <> nvl(trim(S.UM_REG_EARNS),0) or 
T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
T.EMPLID, 
T.INSTITUTION,
T.AID_YEAR, 
T.PAY_END_DT, 
T.EMPL_RCD, 
T.ACCT_CD,
T.DEPTID, 
T.ITEM_TYPE,
T.LAST_RUN_DT,
T.ERN_BEGIN_DT, 
T.ERN_END_DT, 
T.SRC_SYS_ID, 
T.HOURLY_RT,
T.RATE_USED,
T.REG_HRS,
T.REG_EARNS,
T.UM_REG_EARNS, 
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
S.PAY_END_DT, 
S.EMPL_RCD, 
S.ACCT_CD,
S.DEPTID, 
S.ITEM_TYPE,
S.LAST_RUN_DT,
S.ERN_BEGIN_DT, 
S.ERN_END_DT, 
'CS90', 
S.HOURLY_RT,
S.RATE_USED,
S.REG_HRS,
S.REG_EARNS,
S.UM_REG_EARNS, 
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STDNT_WS_ERN rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STDNT_WS_ERN',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_STDNT_WS_ERN';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_WS_ERN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_WS_ERN';
update CSSTG_OWNER.PS_UM_STDNT_WS_ERN T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, PAY_END_DT, EMPL_RCD, ACCT_CD, DEPTID, ITEM_TYPE, LAST_RUN_DT, ERN_BEGIN_DT, ERN_END_DT
   from CSSTG_OWNER.PS_UM_STDNT_WS_ERN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_ERN') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID, 
nvl(trim(INSTITUTION),'-') INSTITUTION, 
nvl(trim(AID_YEAR),'-') AID_YEAR, 
CASE  WHEN TRIM(PAY_END_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE PAY_END_DT
       END  PAY_END_DT,
nvl(EMPL_RCD,0) EMPL_RCD, 
nvl(trim(ACCT_CD),'-') ACCT_CD, 
nvl(trim(DEPTID),'-') DEPTID, 
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE, 
CASE  WHEN TRIM(LAST_RUN_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE LAST_RUN_DT
       END  LAST_RUN_DT,
CASE  WHEN TRIM(ERN_BEGIN_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE ERN_BEGIN_DT
       END  ERN_BEGIN_DT,
CASE  WHEN TRIM(ERN_END_DT) Is Null Then to_date('01-JAN-1900', 'DD-Mon-YYYY')
       ELSE ERN_END_DT
       END  ERN_END_DT
   from SYSADM.PS_UM_STDNT_WS_ERN@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_WS_ERN') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.PAY_END_DT = S.PAY_END_DT
   and T.EMPL_RCD = S.EMPL_RCD
   and T.ACCT_CD = S.ACCT_CD
   and T.DEPTID = S.DEPTID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.LAST_RUN_DT = S.LAST_RUN_DT
   and T.ERN_BEGIN_DT = S.ERN_BEGIN_DT
   and T.ERN_END_DT = S.ERN_END_DT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_STDNT_WS_ERN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_STDNT_WS_ERN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_STDNT_WS_ERN'
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

END PS_UM_STDNT_WS_ERN_P;
/

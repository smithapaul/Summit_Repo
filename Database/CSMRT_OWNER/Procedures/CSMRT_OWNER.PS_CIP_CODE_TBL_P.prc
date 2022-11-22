DROP PROCEDURE CSMRT_OWNER.PS_CIP_CODE_TBL_P
/

--
-- PS_CIP_CODE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CIP_CODE_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_CIP_CODE_TBL from PeopleSoft table PS_CIP_CODE_TBL.
--
-- V01  SMT-xxxx 07/12/2017,    Preethi Lodha
--                              Converted from PS_CIP_CODE_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CIP_CODE_TBL';
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
 where TABLE_NAME = 'PS_CIP_CODE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CIP_CODE_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_CIP_CODE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CIP_CODE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CIP_CODE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CIP_CODE_TBL T
using (select /*+ full(S) */
nvl(trim(CIP_CODE),'-') CIP_CODE,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(CIP_ALTERNATIVE_CD),'-') CIP_ALTERNATIVE_CD,
nvl(trim(CIP_ALTERNATIV_CD2),'-') CIP_ALTERNATIV_CD2,
nvl(trim(COUNTRY_CD),'-') COUNTRY_CD,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCR60),'-') DESCR60,
nvl(trim(DESCR254),'-') DESCR254,
nvl(trim(SEV_VALID_CIP_CD),'-') SEV_VALID_CIP_CD,
nvl(trim(UM_STEM),'-') UM_STEM
from SYSADM.PS_CIP_CODE_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CIP_CODE_TBL') ) S
   on (
T.CIP_CODE = S.CIP_CODE and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.CIP_ALTERNATIVE_CD = S.CIP_ALTERNATIVE_CD,
T.CIP_ALTERNATIV_CD2 = S.CIP_ALTERNATIV_CD2,
T.COUNTRY_CD = S.COUNTRY_CD,
T.DESCR = S.DESCR,
T.DESCR60 = S.DESCR60,
T.DESCR254 = S.DESCR254,
T.SEV_VALID_CIP_CD = S.SEV_VALID_CIP_CD,
T.UM_STEM = S.UM_STEM,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.CIP_ALTERNATIVE_CD <> S.CIP_ALTERNATIVE_CD or
T.CIP_ALTERNATIV_CD2 <> S.CIP_ALTERNATIV_CD2 or
T.COUNTRY_CD <> S.COUNTRY_CD or
T.DESCR <> S.DESCR or
T.DESCR60 <> S.DESCR60 or
T.DESCR254 <> S.DESCR254 or
T.SEV_VALID_CIP_CD <> S.SEV_VALID_CIP_CD or
T.UM_STEM <> S.UM_STEM or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CIP_CODE,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.CIP_ALTERNATIVE_CD,
T.CIP_ALTERNATIV_CD2,
T.COUNTRY_CD,
T.DESCR,
T.DESCR60,
T.DESCR254,
T.SEV_VALID_CIP_CD,
T.UM_STEM,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CIP_CODE,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.CIP_ALTERNATIVE_CD,
S.CIP_ALTERNATIV_CD2,
S.COUNTRY_CD,
S.DESCR,
S.DESCR60,
S.DESCR254,
S.SEV_VALID_CIP_CD,
S.UM_STEM,
'N',
'S',
sysdate,
sysdate,
1234);
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CIP_CODE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CIP_CODE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CIP_CODE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CIP_CODE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CIP_CODE_TBL';
update CSSTG_OWNER.PS_CIP_CODE_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CIP_CODE, EFFDT
   from CSSTG_OWNER.PS_CIP_CODE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CIP_CODE_TBL') = 'Y'
  minus
 select CIP_CODE, EFFDT
   from SYSADM.PS_CIP_CODE_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CIP_CODE_TBL') = 'Y' 
   ) S
 where T.CIP_CODE = S.CIP_CODE    
    AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CIP_CODE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CIP_CODE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CIP_CODE_TBL'
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

END PS_CIP_CODE_TBL_P;
/

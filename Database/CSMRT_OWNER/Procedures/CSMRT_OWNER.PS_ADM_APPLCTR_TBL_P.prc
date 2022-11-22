DROP PROCEDURE CSMRT_OWNER.PS_ADM_APPLCTR_TBL_P
/

--
-- PS_ADM_APPLCTR_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ADM_APPLCTR_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPLCTR_TBL from PeopleSoft table PS_ADM_APPLCTR_TBL.
--
 --V01  SMT-xxxx 08/16/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPLCTR_TBL';
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
 where TABLE_NAME = 'PS_ADM_APPLCTR_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPLCTR_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPLCTR_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPLCTR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPLCTR_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPLCTR_TBL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ADM_APPL_CTR),'-') ADM_APPL_CTR,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(FEE_CODE),'-') FEE_CODE,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(DEPOSIT_FEE_CD),'-') DEPOSIT_FEE_CD,
nvl(trim(SF_MERCHANT_ID),'-') SF_MERCHANT_ID,
nvl(trim(BATCH_APP_FEE_FLG),'-') BATCH_APP_FEE_FLG,
nvl(trim(BATCH_DEP_FEE_FLG),'-') BATCH_DEP_FEE_FLG,
nvl(trim(SAD_CRM_SA_URL_ID),'-') SAD_CRM_SA_URL_ID,
nvl(trim(SAD_CRM_CRM_URL_ID),'-') SAD_CRM_CRM_URL_ID
from SYSADM.PS_ADM_APPLCTR_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPLCTR_TBL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.ADM_APPL_CTR = S.ADM_APPL_CTR and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.FEE_CODE = S.FEE_CODE,
T.ACAD_CAREER = S.ACAD_CAREER,
T.DEPOSIT_FEE_CD = S.DEPOSIT_FEE_CD,
T.SF_MERCHANT_ID = S.SF_MERCHANT_ID,
T.BATCH_APP_FEE_FLG = S.BATCH_APP_FEE_FLG,
T.BATCH_DEP_FEE_FLG = S.BATCH_DEP_FEE_FLG,
T.SAD_CRM_SA_URL_ID = S.SAD_CRM_SA_URL_ID,
T.SAD_CRM_CRM_URL_ID = S.SAD_CRM_CRM_URL_ID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.FEE_CODE <> S.FEE_CODE or
T.ACAD_CAREER <> S.ACAD_CAREER or
T.DEPOSIT_FEE_CD <> S.DEPOSIT_FEE_CD or
T.SF_MERCHANT_ID <> S.SF_MERCHANT_ID or
T.BATCH_APP_FEE_FLG <> S.BATCH_APP_FEE_FLG or
T.BATCH_DEP_FEE_FLG <> S.BATCH_DEP_FEE_FLG or
T.SAD_CRM_SA_URL_ID <> S.SAD_CRM_SA_URL_ID or
T.SAD_CRM_CRM_URL_ID <> S.SAD_CRM_CRM_URL_ID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.ADM_APPL_CTR,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.DESCRSHORT,
T.FEE_CODE,
T.ACAD_CAREER,
T.DEPOSIT_FEE_CD,
T.SF_MERCHANT_ID,
T.BATCH_APP_FEE_FLG,
T.BATCH_DEP_FEE_FLG,
T.SAD_CRM_SA_URL_ID,
T.SAD_CRM_CRM_URL_ID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.ADM_APPL_CTR,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.DESCRSHORT,
S.FEE_CODE,
S.ACAD_CAREER,
S.DEPOSIT_FEE_CD,
S.SF_MERCHANT_ID,
S.BATCH_APP_FEE_FLG,
S.BATCH_DEP_FEE_FLG,
S.SAD_CRM_SA_URL_ID,
S.SAD_CRM_CRM_URL_ID,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPLCTR_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPLCTR_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPLCTR_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPLCTR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPLCTR_TBL';
update CSSTG_OWNER.PS_ADM_APPLCTR_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ADM_APPL_CTR, EFFDT
   from CSSTG_OWNER.PS_ADM_APPLCTR_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPLCTR_TBL') = 'Y'
  minus
 select INSTITUTION, ADM_APPL_CTR, EFFDT
   from SYSADM.PS_ADM_APPLCTR_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPLCTR_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
      and  T.ADM_APPL_CTR = S.ADM_APPL_CTR
      and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPLCTR_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPLCTR_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPLCTR_TBL'
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

END PS_ADM_APPLCTR_TBL_P;
/

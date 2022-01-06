CREATE OR REPLACE PROCEDURE             "PS_EXT_ORG_TBL_ADM_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_EXT_ORG_TBL_ADM from PeopleSoft table PS_EXT_ORG_TBL_ADM.
--
 --V01  SMT-xxxx 10/03/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_EXT_ORG_TBL_ADM';
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
 where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_EXT_ORG_TBL_ADM@SASOURCE S)
 where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_EXT_ORG_TBL_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_EXT_ORG_TBL_ADM';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_EXT_ORG_TBL_ADM T
using (select /*+ full(S) */
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    NVL(EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(SCHOOL_CODE),'-') SCHOOL_CODE, 
    nvl(trim(LS_SCHOOL_TYPE),'-') LS_SCHOOL_TYPE, 
    nvl(trim(FICE_CD),'-') FICE_CD, 
    nvl(trim(ATP_CD),'-') ATP_CD, 
    nvl(trim(OFFERS_COURSES),'-') OFFERS_COURSES, 
    nvl(trim(ACT_CD),'-') ACT_CD, 
    nvl(trim(IPEDS_CD),'-') IPEDS_CD, 
    nvl(trim(SCHOOL_DISTRICT),'-') SCHOOL_DISTRICT, 
    nvl(trim(ACCREDITED),'-') ACCREDITED, 
    nvl(trim(TRANSCRIPT_XLATE),'-') TRANSCRIPT_XLATE, 
    nvl(trim(UNT_TYPE),'-') UNT_TYPE, 
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(trim(SHARED_CATALOG),'-') SHARED_CATALOG, 
    nvl(trim(CATALOG_ORG),'-') CATALOG_ORG, 
    nvl(trim(SCC_NCES_CD),'-') SCC_NCES_CD
from SYSADM.PS_EXT_ORG_TBL_ADM@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM') ) S
 on ( 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.SCHOOL_CODE = S.SCHOOL_CODE,
    T.LS_SCHOOL_TYPE = S.LS_SCHOOL_TYPE,
    T.FICE_CD = S.FICE_CD,
    T.ATP_CD = S.ATP_CD,
    T.OFFERS_COURSES = S.OFFERS_COURSES,
    T.ACT_CD = S.ACT_CD,
    T.IPEDS_CD = S.IPEDS_CD,
    T.SCHOOL_DISTRICT = S.SCHOOL_DISTRICT,
    T.ACCREDITED = S.ACCREDITED,
    T.TRANSCRIPT_XLATE = S.TRANSCRIPT_XLATE,
    T.UNT_TYPE = S.UNT_TYPE,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.EXT_CAREER = S.EXT_CAREER,
    T.SHARED_CATALOG = S.SHARED_CATALOG,
    T.CATALOG_ORG = S.CATALOG_ORG,
    T.SCC_NCES_CD = S.SCC_NCES_CD,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.SCHOOL_CODE <> S.SCHOOL_CODE or 
    T.LS_SCHOOL_TYPE <> S.LS_SCHOOL_TYPE or 
    T.FICE_CD <> S.FICE_CD or 
    T.ATP_CD <> S.ATP_CD or 
    T.OFFERS_COURSES <> S.OFFERS_COURSES or 
    T.ACT_CD <> S.ACT_CD or 
    T.IPEDS_CD <> S.IPEDS_CD or 
    T.SCHOOL_DISTRICT <> S.SCHOOL_DISTRICT or 
    T.ACCREDITED <> S.ACCREDITED or 
    T.TRANSCRIPT_XLATE <> S.TRANSCRIPT_XLATE or 
    T.UNT_TYPE <> S.UNT_TYPE or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.EXT_CAREER <> S.EXT_CAREER or 
    T.SHARED_CATALOG <> S.SHARED_CATALOG or 
    T.CATALOG_ORG <> S.CATALOG_ORG or 
    T.SCC_NCES_CD <> S.SCC_NCES_CD or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EXT_ORG_ID, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.SCHOOL_CODE,
    T.LS_SCHOOL_TYPE, 
    T.FICE_CD,
    T.ATP_CD, 
    T.OFFERS_COURSES, 
    T.ACT_CD, 
    T.IPEDS_CD, 
    T.SCHOOL_DISTRICT,
    T.ACCREDITED, 
    T.TRANSCRIPT_XLATE, 
    T.UNT_TYPE, 
    T.EXT_TERM_TYPE,
    T.EXT_CAREER, 
    T.SHARED_CATALOG, 
    T.CATALOG_ORG,
    T.SCC_NCES_CD,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EXT_ORG_ID, 
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.SCHOOL_CODE,
    S.LS_SCHOOL_TYPE, 
    S.FICE_CD,
    S.ATP_CD, 
    S.OFFERS_COURSES, 
    S.ACT_CD, 
    S.IPEDS_CD, 
    S.SCHOOL_DISTRICT,
    S.ACCREDITED, 
    S.TRANSCRIPT_XLATE, 
    S.UNT_TYPE, 
    S.EXT_TERM_TYPE,
    S.EXT_CAREER, 
    S.SHARED_CATALOG, 
    S.CATALOG_ORG,
    S.SCC_NCES_CD,
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

strMessage01    := '# of PS_EXT_ORG_TBL_ADM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ORG_TBL_ADM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ORG_TBL_ADM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_EXT_ORG_TBL_ADM';
update CSSTG_OWNER.PS_EXT_ORG_TBL_ADM T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EXT_ORG_ID, EFFDT
   from CSSTG_OWNER.PS_EXT_ORG_TBL_ADM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM') = 'Y'
  minus
 select EXT_ORG_ID, EFFDT
   from SYSADM.PS_EXT_ORG_TBL_ADM@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM') = 'Y' 
   ) S
 where T.EXT_ORG_ID = S.EXT_ORG_ID
   AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_EXT_ORG_TBL_ADM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_EXT_ORG_TBL_ADM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_EXT_ORG_TBL_ADM'
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

END PS_EXT_ORG_TBL_ADM_P;
/

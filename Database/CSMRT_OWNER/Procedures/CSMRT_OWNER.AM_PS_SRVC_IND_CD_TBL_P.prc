DROP PROCEDURE CSMRT_OWNER.AM_PS_SRVC_IND_CD_TBL_P
/

--
-- AM_PS_SRVC_IND_CD_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SRVC_IND_CD_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SRVC_IND_CD_TBL from PeopleSoft table PS_SRVC_IND_CD_TBL.
--
 --V01  SMT-xxxx 09/11/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SRVC_IND_CD_TBL';
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
 where TABLE_NAME = 'PS_SRVC_IND_CD_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SRVC_IND_CD_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_SRVC_IND_CD_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_SRVC_IND_CD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SRVC_IND_CD_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SRVC_IND_CD_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(SRVC_IND_CD),'-') SRVC_IND_CD, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(POS_SRVC_INDICATOR),'-') POS_SRVC_INDICATOR, 
    nvl(trim(SCC_HOLD_DISPLAY),'-') SCC_HOLD_DISPLAY, 
    nvl(trim(SCC_SI_PERS),'-') SCC_SI_PERS, 
    nvl(trim(SCC_SI_ORG),'-') SCC_SI_ORG, 
    nvl(trim(SCC_DFLT_ACTDATE),'-') SCC_DFLT_ACTDATE, 
    nvl(trim(SCC_DFLT_ACTTERM),'-') SCC_DFLT_ACTTERM, 
    nvl(trim(DFLT_SRVC_IND_RSN),'-') DFLT_SRVC_IND_RSN, 
    nvl(trim(SRV_IND_DCSD_FLAG),'-') SRV_IND_DCSD_FLAG
from SYSADM.PS_SRVC_IND_CD_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_CD_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.SRVC_IND_CD = S.SRVC_IND_CD and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.POS_SRVC_INDICATOR = S.POS_SRVC_INDICATOR,
    T.SCC_HOLD_DISPLAY = S.SCC_HOLD_DISPLAY,
    T.SCC_SI_PERS = S.SCC_SI_PERS,
    T.SCC_SI_ORG = S.SCC_SI_ORG,
    T.SCC_DFLT_ACTDATE = S.SCC_DFLT_ACTDATE,
    T.SCC_DFLT_ACTTERM = S.SCC_DFLT_ACTTERM,
    T.DFLT_SRVC_IND_RSN = S.DFLT_SRVC_IND_RSN,
    T.SRV_IND_DCSD_FLAG = S.SRV_IND_DCSD_FLAG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.POS_SRVC_INDICATOR <> S.POS_SRVC_INDICATOR or 
    T.SCC_HOLD_DISPLAY <> S.SCC_HOLD_DISPLAY or 
    T.SCC_SI_PERS <> S.SCC_SI_PERS or 
    T.SCC_SI_ORG <> S.SCC_SI_ORG or 
    T.SCC_DFLT_ACTDATE <> S.SCC_DFLT_ACTDATE or 
    T.SCC_DFLT_ACTTERM <> S.SCC_DFLT_ACTTERM or 
    T.DFLT_SRVC_IND_RSN <> S.DFLT_SRVC_IND_RSN or 
    T.SRV_IND_DCSD_FLAG <> S.SRV_IND_DCSD_FLAG or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.SRVC_IND_CD,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.POS_SRVC_INDICATOR, 
    T.SCC_HOLD_DISPLAY, 
    T.SCC_SI_PERS,
    T.SCC_SI_ORG, 
    T.SCC_DFLT_ACTDATE, 
    T.SCC_DFLT_ACTTERM, 
    T.DFLT_SRVC_IND_RSN,
    T.SRV_IND_DCSD_FLAG,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.SRVC_IND_CD,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.POS_SRVC_INDICATOR, 
    S.SCC_HOLD_DISPLAY, 
    S.SCC_SI_PERS,
    S.SCC_SI_ORG, 
    S.SCC_DFLT_ACTDATE, 
    S.SCC_DFLT_ACTTERM, 
    S.DFLT_SRVC_IND_RSN,
    S.SRV_IND_DCSD_FLAG,
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

strMessage01    := '# of PS_SRVC_IND_CD_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SRVC_IND_CD_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SRVC_IND_CD_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SRVC_IND_CD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SRVC_IND_CD_TBL';
update AMSTG_OWNER.PS_SRVC_IND_CD_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, SRVC_IND_CD, EFFDT
   from AMSTG_OWNER.PS_SRVC_IND_CD_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_CD_TBL') = 'Y'
  minus
 select INSTITUTION, SRVC_IND_CD, EFFDT
   from SYSADM.PS_SRVC_IND_CD_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_CD_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.SRVC_IND_CD = S.SRVC_IND_CD
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SRVC_IND_CD_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SRVC_IND_CD_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SRVC_IND_CD_TBL'
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

END AM_PS_SRVC_IND_CD_TBL_P;
/

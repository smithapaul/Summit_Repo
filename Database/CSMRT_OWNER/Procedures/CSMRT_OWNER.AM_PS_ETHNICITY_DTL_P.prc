DROP PROCEDURE CSMRT_OWNER.AM_PS_ETHNICITY_DTL_P
/

--
-- AM_PS_ETHNICITY_DTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ETHNICITY_DTL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ETHNICITY_DTL from PeopleSoft table PS_ETHNICITY_DTL.
--
 --V01  SMT-xxxx 09/01/2017,    James Doucette
--                              Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ETHNICITY_DTL';
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
 where TABLE_NAME = 'PS_ETHNICITY_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ETHNICITY_DTL@AMSOURCE S)
 where TABLE_NAME = 'PS_ETHNICITY_DTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ETHNICITY_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ETHNICITY_DTL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ETHNICITY_DTL T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(REG_REGION),'-') REG_REGION, 
    nvl(trim(ETHNIC_GRP_CD),'-') ETHNIC_GRP_CD, 
    nvl(ETHNIC_PCT_NUMRATR,0) ETHNIC_PCT_NUMRATR, 
    nvl(ETHNIC_PCT_DENMRTR,0) ETHNIC_PCT_DENMRTR, 
    nvl(trim(ETH_VALIDATED),'-') ETH_VALIDATED, 
    nvl(trim(HISP_LATINO),'-') HISP_LATINO, 
    to_date(to_char(case when LASTUPDDTTM < '01-JAN-1800' then NULL else LASTUPDDTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
from SYSADM.PS_ETHNICITY_DTL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ETHNICITY_DTL') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.REG_REGION = S.REG_REGION and 
    T.ETHNIC_GRP_CD = S.ETHNIC_GRP_CD and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ETHNIC_PCT_NUMRATR = S.ETHNIC_PCT_NUMRATR,
    T.ETHNIC_PCT_DENMRTR = S.ETHNIC_PCT_DENMRTR,
    T.ETH_VALIDATED = S.ETH_VALIDATED,
    T.HISP_LATINO = S.HISP_LATINO,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ETHNIC_PCT_NUMRATR <> S.ETHNIC_PCT_NUMRATR or 
    T.ETHNIC_PCT_DENMRTR <> S.ETHNIC_PCT_DENMRTR or 
    T.ETH_VALIDATED <> S.ETH_VALIDATED or 
    T.HISP_LATINO <> S.HISP_LATINO or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.REG_REGION, 
    T.ETHNIC_GRP_CD,
    T.SRC_SYS_ID, 
    T.ETHNIC_PCT_NUMRATR, 
    T.ETHNIC_PCT_DENMRTR, 
    T.ETH_VALIDATED,
    T.HISP_LATINO,
    T.LASTUPDDTTM,
    T.LASTUPDOPRID, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.REG_REGION, 
    S.ETHNIC_GRP_CD,
    'CS90', 
    S.ETHNIC_PCT_NUMRATR, 
    S.ETHNIC_PCT_DENMRTR, 
    S.ETH_VALIDATED,
    S.HISP_LATINO,
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
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

strMessage01    := '# of PS_ETHNICITY_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ETHNICITY_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ETHNICITY_DTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ETHNICITY_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ETHNICITY_DTL';
update AMSTG_OWNER.PS_ETHNICITY_DTL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, REG_REGION, ETHNIC_GRP_CD
   from AMSTG_OWNER.PS_ETHNICITY_DTL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ETHNICITY_DTL') = 'Y'
  minus
 select EMPLID, REG_REGION, ETHNIC_GRP_CD
   from SYSADM.PS_ETHNICITY_DTL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ETHNICITY_DTL') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   and T.REG_REGION = S.REG_REGION
   and T.ETHNIC_GRP_CD = S.ETHNIC_GRP_CD
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ETHNICITY_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ETHNICITY_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ETHNICITY_DTL'
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

END AM_PS_ETHNICITY_DTL_P;
/

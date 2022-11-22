DROP PROCEDURE CSMRT_OWNER.AM_PS_PERS_DATA_EFFDT_P
/

--
-- AM_PS_PERS_DATA_EFFDT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_PERS_DATA_EFFDT_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PERS_DATA_EFFDT from PeopleSoft table PS_PERS_DATA_EFFDT.
--
 --V01  SMT-xxxx 08/17/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_PERS_DATA_EFFDT';
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
 where TABLE_NAME = 'PS_PERS_DATA_EFFDT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PERS_DATA_EFFDT@AMSOURCE S)
 where TABLE_NAME = 'PS_PERS_DATA_EFFDT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_PERS_DATA_EFFDT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_PERS_DATA_EFFDT';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_PERS_DATA_EFFDT T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    EFFDT, 
    nvl(trim(MAR_STATUS),'-') MAR_STATUS, 
    MAR_STATUS_DT, 
    nvl(trim(SEX),'-') SEX, 
    nvl(trim(HIGHEST_EDUC_LVL),'-') HIGHEST_EDUC_LVL, 
    nvl(trim(FT_STUDENT),'-') FT_STUDENT, 
    nvl(trim(LANG_CD),'-') LANG_CD, 
    nvl(trim(ALTER_EMPLID),'-') ALTER_EMPLID, 
    LASTUPDDTTM, 
    nvl(trim(LASTUPDOPRID),'-') LASTUPDOPRID
from SYSADM.PS_PERS_DATA_EFFDT@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_EFFDT') 
  and EMPLID between '00000000' and '99999999'
  and length(trim(EMPLID)) = 8) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.MAR_STATUS = S.MAR_STATUS,
    T.MAR_STATUS_DT = S.MAR_STATUS_DT,
    T.SEX = S.SEX,
    T.HIGHEST_EDUC_LVL = S.HIGHEST_EDUC_LVL,
    T.FT_STUDENT = S.FT_STUDENT,
    T.LANG_CD = S.LANG_CD,
    T.ALTER_EMPLID = S.ALTER_EMPLID,
    T.LASTUPDDTTM = S.LASTUPDDTTM,
    T.LASTUPDOPRID = S.LASTUPDOPRID,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.MAR_STATUS <> S.MAR_STATUS or 
    nvl(trim(T.MAR_STATUS_DT),0) <> nvl(trim(S.MAR_STATUS_DT),0) or 
    T.SEX <> S.SEX or 
    T.HIGHEST_EDUC_LVL <> S.HIGHEST_EDUC_LVL or 
    T.FT_STUDENT <> S.FT_STUDENT or 
    T.LANG_CD <> S.LANG_CD or 
    T.ALTER_EMPLID <> S.ALTER_EMPLID or 
    nvl(trim(T.LASTUPDDTTM),0) <> nvl(trim(S.LASTUPDDTTM),0) or 
    T.LASTUPDOPRID <> S.LASTUPDOPRID or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.MAR_STATUS, 
    T.MAR_STATUS_DT,
    T.SEX,
    T.HIGHEST_EDUC_LVL, 
    T.FT_STUDENT, 
    T.LANG_CD,
    T.ALTER_EMPLID, 
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
    S.EFFDT,
    'CS90', 
    S.MAR_STATUS, 
    S.MAR_STATUS_DT,
    S.SEX,
    S.HIGHEST_EDUC_LVL, 
    S.FT_STUDENT, 
    S.LANG_CD,
    S.ALTER_EMPLID, 
    S.LASTUPDDTTM,
    S.LASTUPDOPRID, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

commit;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PERS_DATA_EFFDT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERS_DATA_EFFDT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PERS_DATA_EFFDT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_PERS_DATA_EFFDT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_PERS_DATA_EFFDT';
update AMSTG_OWNER.PS_PERS_DATA_EFFDT T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EFFDT
   from AMSTG_OWNER.PS_PERS_DATA_EFFDT T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_EFFDT') = 'Y'
  minus
 select EMPLID, EFFDT
   from SYSADM.PS_PERS_DATA_EFFDT@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERS_DATA_EFFDT') = 'Y'
    and EMPLID between '00000000' and '99999999'
    and length(trim(EMPLID)) = 8)
    S
 where T.EMPLID = S.EMPLID
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_PERS_DATA_EFFDT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERS_DATA_EFFDT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PERS_DATA_EFFDT'
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

END AM_PS_PERS_DATA_EFFDT_P;
/

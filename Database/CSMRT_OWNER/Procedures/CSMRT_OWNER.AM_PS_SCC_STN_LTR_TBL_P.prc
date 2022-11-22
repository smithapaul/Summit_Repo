DROP PROCEDURE CSMRT_OWNER.AM_PS_SCC_STN_LTR_TBL_P
/

--
-- AM_PS_SCC_STN_LTR_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SCC_STN_LTR_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SCC_STN_LTR_TBL from PeopleSoft table PS_SCC_STN_LTR_TBL.
--
 --V01  SMT-xxxx 09/08/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SCC_STN_LTR_TBL';
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
 where TABLE_NAME = 'PS_SCC_STN_LTR_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SCC_STN_LTR_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_SCC_STN_LTR_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_SCC_STN_LTR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SCC_STN_LTR_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SCC_STN_LTR_TBL T
using (select /*+ full(S) */
    nvl(trim(SCC_LETTER_CD),'-') SCC_LETTER_CD, 
    nvl(trim(SCC_SET_LETTER_CD),'-') SCC_SET_LETTER_CD, 
    replace(nvl(trim(DESCR),'-'), '  ', ' ') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION, 
    nvl(trim(SQC_NAME),'-') SQC_NAME, 
    nvl(trim(ENCL_TYPE),'-') ENCL_TYPE, 
    nvl(trim(LTR_PRINT_DATA_OPT),'-') LTR_PRINT_DATA_OPT, 
    nvl(trim(ALLOW_JOINT),'-') ALLOW_JOINT, 
    nvl(trim(INCLUDE_ENCL),'-') INCLUDE_ENCL
from SYSADM.PS_SCC_STN_LTR_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_STN_LTR_TBL') ) S
 on ( 
    T.SCC_LETTER_CD = S.SCC_LETTER_CD and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SCC_SET_LETTER_CD = S.SCC_SET_LETTER_CD,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,
    T.SQC_NAME = S.SQC_NAME,
    T.ENCL_TYPE = S.ENCL_TYPE,
    T.LTR_PRINT_DATA_OPT = S.LTR_PRINT_DATA_OPT,
    T.ALLOW_JOINT = S.ALLOW_JOINT,
    T.INCLUDE_ENCL = S.INCLUDE_ENCL,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SCC_SET_LETTER_CD <> S.SCC_SET_LETTER_CD or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.ADMIN_FUNCTION <> S.ADMIN_FUNCTION or 
    T.SQC_NAME <> S.SQC_NAME or 
    T.ENCL_TYPE <> S.ENCL_TYPE or 
    T.LTR_PRINT_DATA_OPT <> S.LTR_PRINT_DATA_OPT or 
    T.ALLOW_JOINT <> S.ALLOW_JOINT or 
    T.INCLUDE_ENCL <> S.INCLUDE_ENCL or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.SCC_LETTER_CD,
    T.SRC_SYS_ID, 
    T.SCC_SET_LETTER_CD,
    T.DESCR,
    T.DESCRSHORT, 
    T.ADMIN_FUNCTION, 
    T.SQC_NAME, 
    T.ENCL_TYPE,
    T.LTR_PRINT_DATA_OPT, 
    T.ALLOW_JOINT,
    T.INCLUDE_ENCL, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.SCC_LETTER_CD,
    'CS90', 
    S.SCC_SET_LETTER_CD,
    S.DESCR,
    S.DESCRSHORT, 
    S.ADMIN_FUNCTION, 
    S.SQC_NAME, 
    S.ENCL_TYPE,
    S.LTR_PRINT_DATA_OPT, 
    S.ALLOW_JOINT,
    S.INCLUDE_ENCL, 
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

strMessage01    := '# of PS_SCC_STN_LTR_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCC_STN_LTR_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SCC_STN_LTR_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SCC_STN_LTR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SCC_STN_LTR_TBL';
update AMSTG_OWNER.PS_SCC_STN_LTR_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SCC_LETTER_CD
   from AMSTG_OWNER.PS_SCC_STN_LTR_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_STN_LTR_TBL') = 'Y'
  minus
 select SCC_LETTER_CD
   from SYSADM.PS_SCC_STN_LTR_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCC_STN_LTR_TBL') = 'Y'
   ) S
 where T.SCC_LETTER_CD = S.SCC_LETTER_CD
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SCC_STN_LTR_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCC_STN_LTR_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SCC_STN_LTR_TBL'
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

END AM_PS_SCC_STN_LTR_TBL_P;
/

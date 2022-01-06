CREATE OR REPLACE PROCEDURE             "PS_ACAD_LOAD_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_ACAD_LOAD_TBL from PeopleSoft table PS_ACAD_LOAD_TBL.
--
-- V01  SMT-xxxx 07/12/2017,    Preethi Lodha
--                              Converted from PS_ACAD_LOAD_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_LOAD_TBL';
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
 where TABLE_NAME = 'PS_ACAD_LOAD_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_LOAD_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_LOAD_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_LOAD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_LOAD_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_LOAD_TBL T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(LEVEL_LOAD_RULE),'-') LEVEL_LOAD_RULE,
EFFDT,
nvl(trim(TERM_CATEGORY),'-') TERM_CATEGORY,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(UNT_TRM_TOTAL,0) UNT_TRM_TOTAL,
nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD,
nvl(trim(FA_LOAD),'-') FA_LOAD,
nvl(trim(ACADEMIC_LOAD_NSLC),'-') ACADEMIC_LOAD_NSLC,
nvl(COURSE_LD_PCT,0) COURSE_LD_PCT,
nvl(RES_TERMS_ADJ,0) RES_TERMS_ADJ
from SYSADM.PS_ACAD_LOAD_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LOAD_TBL') ) S
   on (
T.SETID = S.SETID and
T.LEVEL_LOAD_RULE = S.LEVEL_LOAD_RULE and
T.EFFDT = S.EFFDT and
T.TERM_CATEGORY = S.TERM_CATEGORY and
T.SESSION_CODE = S.SESSION_CODE and
T.UNT_TRM_TOTAL = S.UNT_TRM_TOTAL and
T.ACADEMIC_LOAD = S.ACADEMIC_LOAD and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.FA_LOAD = S.FA_LOAD,
T.ACADEMIC_LOAD_NSLC = S.ACADEMIC_LOAD_NSLC,
T.COURSE_LD_PCT = S.COURSE_LD_PCT,
T.RES_TERMS_ADJ = S.RES_TERMS_ADJ,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.FA_LOAD <> S.FA_LOAD or
T.ACADEMIC_LOAD_NSLC <> S.ACADEMIC_LOAD_NSLC or
T.COURSE_LD_PCT <> S.COURSE_LD_PCT or
T.RES_TERMS_ADJ <> S.RES_TERMS_ADJ or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.LEVEL_LOAD_RULE,
T.EFFDT,
T.TERM_CATEGORY,
T.SESSION_CODE,
T.UNT_TRM_TOTAL,
T.ACADEMIC_LOAD,
T.SRC_SYS_ID,
T.FA_LOAD,
T.ACADEMIC_LOAD_NSLC,
T.COURSE_LD_PCT,
T.RES_TERMS_ADJ,
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
S.TERM_CATEGORY,
S.SESSION_CODE,
S.UNT_TRM_TOTAL,
S.ACADEMIC_LOAD,
'CS90',
S.FA_LOAD,
S.ACADEMIC_LOAD_NSLC,
S.COURSE_LD_PCT,
S.RES_TERMS_ADJ,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_LOAD_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_LOAD_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_LOAD_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_LOAD_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_LOAD_TBL';

update CSSTG_OWNER.PS_ACAD_LOAD_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(SETID),'-') SETID,
        nvl(trim(LEVEL_LOAD_RULE),'-') LEVEL_LOAD_RULE,
        EFFDT,
        nvl(trim(TERM_CATEGORY),'-') TERM_CATEGORY,
        nvl(trim(SESSION_CODE),'-') SESSION_CODE,
        nvl(UNT_TRM_TOTAL,0) UNT_TRM_TOTAL,
        nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD
   from CSSTG_OWNER.PS_ACAD_LOAD_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LOAD_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID,
        nvl(trim(LEVEL_LOAD_RULE),'-') LEVEL_LOAD_RULE,
        EFFDT,
        nvl(trim(TERM_CATEGORY),'-') TERM_CATEGORY,
        nvl(trim(SESSION_CODE),'-') SESSION_CODE,
        nvl(UNT_TRM_TOTAL,0) UNT_TRM_TOTAL,
        nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD
   from SYSADM.PS_ACAD_LOAD_TBL@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_LOAD_TBL') = 'Y' 
   ) S
 where nvl(trim(T.SETID),'-') = nvl(trim(S.SETID),'-')
    AND nvl(trim(LEVEL_LOAD_RULE),'-')  = nvl(trim(S.LEVEL_LOAD_RULE),'-')     
    AND T.EFFDT = S.EFFDT
    AND nvl(trim(T.TERM_CATEGORY),'-')  = nvl(trim(S.TERM_CATEGORY),'-') 
    AND nvl(trim(T.SESSION_CODE),'-') = nvl(trim(S.SESSION_CODE),'-')
    AND nvl(T.UNT_TRM_TOTAL,0) = nvl(S.UNT_TRM_TOTAL,0)
    AND nvl(trim(T.ACADEMIC_LOAD),'-') = nvl(trim(S.ACADEMIC_LOAD),'-')
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_LOAD_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_LOAD_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_LOAD_TBL'
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

END PS_ACAD_LOAD_TBL_P;
/

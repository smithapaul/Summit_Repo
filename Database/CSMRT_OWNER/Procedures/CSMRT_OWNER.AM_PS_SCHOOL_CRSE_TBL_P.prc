DROP PROCEDURE CSMRT_OWNER.AM_PS_SCHOOL_CRSE_TBL_P
/

--
-- AM_PS_SCHOOL_CRSE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_SCHOOL_CRSE_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SCHOOL_CRSE_TBL from PeopleSoft table PS_SCHOOL_CRSE_TBL.
--
 --V01  SMT-xxxx 10/03/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_SCHOOL_CRSE_TBL';
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
 where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SCHOOL_CRSE_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_SCHOOL_CRSE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_SCHOOL_CRSE_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_SCHOOL_CRSE_TBL T
using (select /*+ full(S) */
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(SCHOOL_SUBJECT),'-') SCHOOL_SUBJECT, 
    nvl(trim(SCHOOL_CRSE_NBR),'-') SCHOOL_CRSE_NBR, 
    NVL(EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(EXT_SUBJECT_AREA),'-') EXT_SUBJECT_AREA, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(trim(EXT_CRSE_TYPE),'-') EXT_CRSE_TYPE, 
    nvl(trim(COURSE_LEVEL),'-') COURSE_LEVEL, 
    replace(nvl(trim(DESCR),'-'), '  ', ' ') DESCR, 
    replace(nvl(trim(DESCRSHORT),'-'), '  ', ' ') DESCRSHORT, 
    nvl(EXT_UNITS,0) EXT_UNITS
from SYSADM.PS_SCHOOL_CRSE_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL') ) S
 on ( 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.SCHOOL_SUBJECT = S.SCHOOL_SUBJECT and 
    T.SCHOOL_CRSE_NBR = S.SCHOOL_CRSE_NBR and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.EXT_SUBJECT_AREA = S.EXT_SUBJECT_AREA,
    T.EXT_CAREER = S.EXT_CAREER,
    T.EXT_CRSE_TYPE = S.EXT_CRSE_TYPE,
    T.COURSE_LEVEL = S.COURSE_LEVEL,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.EXT_UNITS = S.EXT_UNITS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.EXT_SUBJECT_AREA <> S.EXT_SUBJECT_AREA or 
    T.EXT_CAREER <> S.EXT_CAREER or 
    T.EXT_CRSE_TYPE <> S.EXT_CRSE_TYPE or 
    T.COURSE_LEVEL <> S.COURSE_LEVEL or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.EXT_UNITS <> S.EXT_UNITS or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EXT_ORG_ID, 
    T.SCHOOL_SUBJECT, 
    T.SCHOOL_CRSE_NBR,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.EXT_SUBJECT_AREA, 
    T.EXT_CAREER, 
    T.EXT_CRSE_TYPE,
    T.COURSE_LEVEL, 
    T.DESCR,
    T.DESCRSHORT, 
    T.EXT_UNITS,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EXT_ORG_ID, 
    S.SCHOOL_SUBJECT, 
    S.SCHOOL_CRSE_NBR,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.EXT_SUBJECT_AREA, 
    S.EXT_CAREER, 
    S.EXT_CRSE_TYPE,
    S.COURSE_LEVEL, 
    S.DESCR,
    S.DESCRSHORT, 
    S.EXT_UNITS,
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

strMessage01    := '# of PS_SCHOOL_CRSE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCHOOL_CRSE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_SCHOOL_CRSE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_SCHOOL_CRSE_TBL';
update AMSTG_OWNER.PS_SCHOOL_CRSE_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EXT_ORG_ID, SCHOOL_SUBJECT, SCHOOL_CRSE_NBR, EFFDT
   from AMSTG_OWNER.PS_SCHOOL_CRSE_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL') = 'Y'
  minus
 select EXT_ORG_ID, SCHOOL_SUBJECT, SCHOOL_CRSE_NBR, NVL(EFFDT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) EFFDT
   from SYSADM.PS_SCHOOL_CRSE_TBL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL') = 'Y' 
   ) S
 where T.EXT_ORG_ID = S.EXT_ORG_ID
   and T.SCHOOL_SUBJECT = S.SCHOOL_SUBJECT
   and T.SCHOOL_CRSE_NBR = S.SCHOOL_CRSE_NBR
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SCHOOL_CRSE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SCHOOL_CRSE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SCHOOL_CRSE_TBL'
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

END AM_PS_SCHOOL_CRSE_TBL_P;
/

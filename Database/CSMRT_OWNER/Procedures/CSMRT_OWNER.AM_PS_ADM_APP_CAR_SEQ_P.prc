DROP PROCEDURE CSMRT_OWNER.AM_PS_ADM_APP_CAR_SEQ_P
/

--
-- AM_PS_ADM_APP_CAR_SEQ_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ADM_APP_CAR_SEQ_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APP_CAR_SEQ from PeopleSoft table PS_ADM_APP_CAR_SEQ.
--
-- V01  SMT-xxxx 06/05/2017,    Jim Doucette
--                              Converted from PS_ADM_APP_CAR_SEQ.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ADM_APP_CAR_SEQ';
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
 where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APP_CAR_SEQ@AMSOURCE S)
 where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ADM_APP_CAR_SEQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ADM_APP_CAR_SEQ';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ADM_APP_CAR_SEQ T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(APPL_PROG_NBR,0) APPL_PROG_NBR, 
    nvl(trim(EVALUATN_STATUS),'-') EVALUATN_STATUS, 
    to_date(to_char(case when EVALUATION_DT < '01-JAN-1800' then NULL else EVALUATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EVALUATION_DT, 
    nvl(trim(EVALUATION_CHG),'-') EVALUATION_CHG, 
    nvl(trim(SUMMARY_NEW),'-') SUMMARY_NEW, 
    nvl(trim(TEST_SCORE_NEW),'-') TEST_SCORE_NEW, 
    nvl(trim(COURSE_NEW),'-') COURSE_NEW, 
    nvl(trim(SUBJECT_NEW),'-') SUBJECT_NEW, 
    nvl(trim(GENL_MATL_NEW),'-') GENL_MATL_NEW, 
    nvl(trim(AUTO_UPDATE),'-') AUTO_UPDATE, 
    nvl(STDNT_CAR_NBR_SR,0) STDNT_CAR_NBR_SR, 
    nvl(trim(CREATE_PROG_STATUS),'-') CREATE_PROG_STATUS, 
    nvl(trim(DEP_CALC_NEEDED),'-') DEP_CALC_NEEDED, 
    to_date(to_char(case when DEP_CALC_DT < '01-JAN-1800' then NULL 
                    else DEP_CALC_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DEP_CALC_DT, 
    nvl(PROCESS_INSTANCE,0) PROCESS_INSTANCE
from SYSADM.PS_ADM_APP_CAR_SEQ@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S
on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.APPL_PROG_NBR = S.APPL_PROG_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EVALUATN_STATUS = S.EVALUATN_STATUS,
    T.EVALUATION_DT = S.EVALUATION_DT,
    T.EVALUATION_CHG = S.EVALUATION_CHG,
    T.SUMMARY_NEW = S.SUMMARY_NEW,
    T.TEST_SCORE_NEW = S.TEST_SCORE_NEW,
    T.COURSE_NEW = S.COURSE_NEW,
    T.SUBJECT_NEW = S.SUBJECT_NEW,
    T.GENL_MATL_NEW = S.GENL_MATL_NEW,
    T.AUTO_UPDATE = S.AUTO_UPDATE,
    T.STDNT_CAR_NBR_SR = S.STDNT_CAR_NBR_SR,
    T.CREATE_PROG_STATUS = S.CREATE_PROG_STATUS,
    T.DEP_CALC_NEEDED = S.DEP_CALC_NEEDED,
    T.DEP_CALC_DT = S.DEP_CALC_DT,
    T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EVALUATN_STATUS <> S.EVALUATN_STATUS or 
    nvl(trim(T.EVALUATION_DT),0) <> nvl(trim(S.EVALUATION_DT),0) or 
    T.EVALUATION_CHG <> S.EVALUATION_CHG or 
    T.SUMMARY_NEW <> S.SUMMARY_NEW or 
    T.TEST_SCORE_NEW <> S.TEST_SCORE_NEW or 
    T.COURSE_NEW <> S.COURSE_NEW or 
    T.SUBJECT_NEW <> S.SUBJECT_NEW or 
    T.GENL_MATL_NEW <> S.GENL_MATL_NEW or 
    T.AUTO_UPDATE <> S.AUTO_UPDATE or 
    T.STDNT_CAR_NBR_SR <> S.STDNT_CAR_NBR_SR or 
    T.CREATE_PROG_STATUS <> S.CREATE_PROG_STATUS or 
    T.DEP_CALC_NEEDED <> S.DEP_CALC_NEEDED or 
    nvl(trim(T.DEP_CALC_DT),0) <> nvl(trim(S.DEP_CALC_DT),0) or 
    T.PROCESS_INSTANCE <> S.PROCESS_INSTANCE or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.APPL_PROG_NBR,
    T.SRC_SYS_ID, 
    T.EVALUATN_STATUS,
    T.EVALUATION_DT,
    T.EVALUATION_CHG, 
    T.SUMMARY_NEW,
    T.TEST_SCORE_NEW, 
    T.COURSE_NEW, 
    T.SUBJECT_NEW,
    T.GENL_MATL_NEW,
    T.AUTO_UPDATE,
    T.STDNT_CAR_NBR_SR, 
    T.CREATE_PROG_STATUS, 
    T.DEP_CALC_NEEDED,
    T.DEP_CALC_DT,
    T.PROCESS_INSTANCE, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    S.APPL_PROG_NBR,
    'CS90', 
    S.EVALUATN_STATUS,
    NVL(S.EVALUATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    S.EVALUATION_CHG, 
    S.SUMMARY_NEW,
    S.TEST_SCORE_NEW, 
    S.COURSE_NEW, 
    S.SUBJECT_NEW,
    S.GENL_MATL_NEW,
    S.AUTO_UPDATE,
    S.STDNT_CAR_NBR_SR, 
    S.CREATE_PROG_STATUS, 
    S.DEP_CALC_NEEDED,
    NVL(S.DEP_CALC_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )),
    S.PROCESS_INSTANCE, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APP_CAR_SEQ rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APP_CAR_SEQ',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ADM_APP_CAR_SEQ';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ADM_APP_CAR_SEQ';
update AMSTG_OWNER.PS_ADM_APP_CAR_SEQ T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR
   from AMSTG_OWNER.PS_ADM_APP_CAR_SEQ T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR
   from SYSADM.PS_ADM_APP_CAR_SEQ@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.APPL_PROG_NBR = S.APPL_PROG_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APP_CAR_SEQ rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APP_CAR_SEQ',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APP_CAR_SEQ'
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

END AM_PS_ADM_APP_CAR_SEQ_P;
/

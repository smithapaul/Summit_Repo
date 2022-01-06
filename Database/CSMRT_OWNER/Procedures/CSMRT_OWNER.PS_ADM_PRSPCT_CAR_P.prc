CREATE OR REPLACE PROCEDURE             "PS_ADM_PRSPCT_CAR_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_PRSPCT_CAR from PeopleSoft table PS_ADM_PRSPCT_CAR.
--
 --V01  SMT-xxxx 10/02/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_PRSPCT_CAR';
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
 where TABLE_NAME = 'PS_ADM_PRSPCT_CAR'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_PRSPCT_CAR@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_PRSPCT_CAR'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_PRSPCT_CAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_PRSPCT_CAR';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_PRSPCT_CAR T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ADMIT_TERM),'-') ADMIT_TERM, 
    nvl(trim(ADMIT_TYPE),'-') ADMIT_TYPE, 
    nvl(trim(ADM_RECR_CTR),'-') ADM_RECR_CTR, 
    nvl(trim(LAST_SCH_ATTEND),'-') LAST_SCH_ATTEND, 
    NVL(GRADUATION_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) GRADUATION_DT, 
    nvl(trim(RECRUITING_STATUS),'-') RECRUITING_STATUS, 
    RECR_STATUS_DT,
    nvl(trim(APPL_ON_FILE),'-') APPL_ON_FILE, 
    nvl(trim(FIN_AID_INTEREST),'-') FIN_AID_INTEREST, 
    nvl(trim(HOUSING_INTEREST),'-') HOUSING_INTEREST, 
    nvl(trim(ACAD_LOAD_APPR),'-') ACAD_LOAD_APPR, 
    nvl(trim(ADM_REFRL_SRCE),'-') ADM_REFRL_SRCE, 
    NVL(REFERRAL_SRCE_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) REFERRAL_SRCE_DT,
    nvl(trim(REGION),'-') REGION, 
    nvl(trim(REGION_FROM),'-') REGION_FROM, 
    nvl(trim(RECRUITER_ID),'-') RECRUITER_ID, 
    ADM_CREATION_DT, 
    nvl(trim(ACADEMIC_LEVEL),'-') ACADEMIC_LEVEL, 
    nvl(trim(CAMPUS),'-') CAMPUS
from SYSADM.PS_ADM_PRSPCT_CAR@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_CAR')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.ADMIT_TERM = S.ADMIT_TERM,
    T.ADMIT_TYPE = S.ADMIT_TYPE,
    T.ADM_RECR_CTR = S.ADM_RECR_CTR,
    T.LAST_SCH_ATTEND = S.LAST_SCH_ATTEND,
    T.GRADUATION_DT = S.GRADUATION_DT,
    T.RECRUITING_STATUS = S.RECRUITING_STATUS,
    T.RECR_STATUS_DT = S.RECR_STATUS_DT,
    T.APPL_ON_FILE = S.APPL_ON_FILE,
    T.FIN_AID_INTEREST = S.FIN_AID_INTEREST,
    T.HOUSING_INTEREST = S.HOUSING_INTEREST,
    T.ACAD_LOAD_APPR = S.ACAD_LOAD_APPR,
    T.ADM_REFRL_SRCE = S.ADM_REFRL_SRCE,
    T.REFERRAL_SRCE_DT = S.REFERRAL_SRCE_DT,
    T.REGION = S.REGION,
    T.REGION_FROM = S.REGION_FROM,
    T.RECRUITER_ID = S.RECRUITER_ID,
    T.ADM_CREATION_DT = S.ADM_CREATION_DT,
    T.ACADEMIC_LEVEL = S.ACADEMIC_LEVEL,
    T.CAMPUS = S.CAMPUS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.ADMIT_TERM <> S.ADMIT_TERM or 
    T.ADMIT_TYPE <> S.ADMIT_TYPE or 
    T.ADM_RECR_CTR <> S.ADM_RECR_CTR or 
    T.LAST_SCH_ATTEND <> S.LAST_SCH_ATTEND or 
    nvl(trim(T.GRADUATION_DT),0) <> nvl(trim(S.GRADUATION_DT),0) or 
    T.RECRUITING_STATUS <> S.RECRUITING_STATUS or 
    nvl(trim(T.RECR_STATUS_DT),0) <> nvl(trim(S.RECR_STATUS_DT),0) or 
    T.APPL_ON_FILE <> S.APPL_ON_FILE or 
    T.FIN_AID_INTEREST <> S.FIN_AID_INTEREST or 
    T.HOUSING_INTEREST <> S.HOUSING_INTEREST or 
    T.ACAD_LOAD_APPR <> S.ACAD_LOAD_APPR or 
    T.ADM_REFRL_SRCE <> S.ADM_REFRL_SRCE or 
    nvl(trim(T.REFERRAL_SRCE_DT),0) <> nvl(trim(S.REFERRAL_SRCE_DT),0) or 
    T.REGION <> S.REGION or 
    T.REGION_FROM <> S.REGION_FROM or 
    T.RECRUITER_ID <> S.RECRUITER_ID or 
    nvl(trim(T.ADM_CREATION_DT),0) <> nvl(trim(S.ADM_CREATION_DT),0) or 
    T.ACADEMIC_LEVEL <> S.ACADEMIC_LEVEL or 
    T.CAMPUS <> S.CAMPUS or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.SRC_SYS_ID, 
    T.ADMIT_TERM, 
    T.ADMIT_TYPE, 
    T.ADM_RECR_CTR, 
    T.LAST_SCH_ATTEND,
    T.GRADUATION_DT,
    T.RECRUITING_STATUS,
    T.RECR_STATUS_DT, 
    T.APPL_ON_FILE, 
    T.FIN_AID_INTEREST, 
    T.HOUSING_INTEREST, 
    T.ACAD_LOAD_APPR, 
    T.ADM_REFRL_SRCE, 
    T.REFERRAL_SRCE_DT, 
    T.REGION, 
    T.REGION_FROM,
    T.RECRUITER_ID, 
    T.ADM_CREATION_DT,
    T.ACADEMIC_LEVEL, 
    T.CAMPUS, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    'CS90', 
    S.ADMIT_TERM, 
    S.ADMIT_TYPE, 
    S.ADM_RECR_CTR, 
    S.LAST_SCH_ATTEND,
    S.GRADUATION_DT,
    S.RECRUITING_STATUS,
    S.RECR_STATUS_DT, 
    S.APPL_ON_FILE, 
    S.FIN_AID_INTEREST, 
    S.HOUSING_INTEREST, 
    S.ACAD_LOAD_APPR, 
    S.ADM_REFRL_SRCE, 
    S.REFERRAL_SRCE_DT, 
    S.REGION, 
    S.REGION_FROM,
    S.RECRUITER_ID, 
    S.ADM_CREATION_DT,
    S.ACADEMIC_LEVEL, 
    S.CAMPUS, 
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

strMessage01    := '# of PS_ADM_PRSPCT_CAR rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_PRSPCT_CAR',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_PRSPCT_CAR';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_PRSPCT_CAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_PRSPCT_CAR';
update CSSTG_OWNER.PS_ADM_PRSPCT_CAR T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION
   from CSSTG_OWNER.PS_ADM_PRSPCT_CAR T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_CAR') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION
   from SYSADM.PS_ADM_PRSPCT_CAR@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_PRSPCT_CAR') = 'Y' 
   ) S
 where T.EMPLID= S.EMPLID
    AND T.ACAD_CAREER = S.ACAD_CAREER
    AND T.INSTITUTION = S.INSTITUTION
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_PRSPCT_CAR rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_PRSPCT_CAR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_PRSPCT_CAR'
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

END PS_ADM_PRSPCT_CAR_P;
/

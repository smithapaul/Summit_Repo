CREATE OR REPLACE PROCEDURE             "PS_UM_ADM_BHE_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_ADM_BHE from PeopleSoft table PS_UM_ADM_BHE.
--
 --V01  SMT-xxxx 10/06/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_ADM_BHE';
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
 where TABLE_NAME = 'PS_UM_ADM_BHE'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_ADM_BHE@SASOURCE S)
 where TABLE_NAME = 'PS_UM_ADM_BHE'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_ADM_BHE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_ADM_BHE';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_ADM_BHE T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(APPL_PROG_NBR,0) APPL_PROG_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(UM_BHE),'-') UM_BHE, 
    nvl(trim(UM_BHE_ENG),'-') UM_BHE_ENG, 
    nvl(trim(UM_BHE_SOCSCI),'-') UM_BHE_SOCSCI, 
    nvl(trim(UM_BHE_SCI),'-') UM_BHE_SCI, 
    nvl(trim(UM_BHE_MATH),'-') UM_BHE_MATH, 
    nvl(trim(UM_BHE_ELT),'-') UM_BHE_ELT, 
    nvl(trim(UM_BHE_FRLG),'-') UM_BHE_FRLG, 
    nvl(trim(UM_BHE_CMPLT),'-') UM_BHE_CMPLT, 
    nvl(trim(UM_BHE_EXP_VOCTEC),'-') UM_BHE_EXP_VOCTEC, 
    nvl(trim(UM_BHE_EXP_ESL),'-') UM_BHE_EXP_ESL, 
    nvl(trim(UM_BHE_EXP_INTL),'-') UM_BHE_EXP_INTL, 
    nvl(trim(UM_BHE_PRECOLLEGE),'-') UM_BHE_PRECOLLEGE, 
    nvl(trim(UM_BHE_EXP_LD),'-') UM_BHE_EXP_LD, 
    nvl(UM_BHE_TRANS_CR,0) UM_BHE_TRANS_CR, 
    nvl(UM_BHE_TRANS_GPA,0) UM_BHE_TRANS_GPA
from SYSADM.PS_UM_ADM_BHE@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_BHE')
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
    T.INSTITUTION = S.INSTITUTION,
    T.UM_BHE = S.UM_BHE,
    T.UM_BHE_ENG = S.UM_BHE_ENG,
    T.UM_BHE_SOCSCI = S.UM_BHE_SOCSCI,
    T.UM_BHE_SCI = S.UM_BHE_SCI,
    T.UM_BHE_MATH = S.UM_BHE_MATH,
    T.UM_BHE_ELT = S.UM_BHE_ELT,
    T.UM_BHE_FRLG = S.UM_BHE_FRLG,
    T.UM_BHE_CMPLT = S.UM_BHE_CMPLT,
    T.UM_BHE_EXP_VOCTEC = S.UM_BHE_EXP_VOCTEC,
    T.UM_BHE_EXP_ESL = S.UM_BHE_EXP_ESL,
    T.UM_BHE_EXP_INTL = S.UM_BHE_EXP_INTL,
    T.UM_BHE_PRECOLLEGE = S.UM_BHE_PRECOLLEGE,
    T.UM_BHE_EXP_LD = S.UM_BHE_EXP_LD,
    T.UM_BHE_TRANS_CR = S.UM_BHE_TRANS_CR,
    T.UM_BHE_TRANS_GPA = S.UM_BHE_TRANS_GPA,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.UM_BHE <> S.UM_BHE or 
    T.UM_BHE_ENG <> S.UM_BHE_ENG or 
    T.UM_BHE_SOCSCI <> S.UM_BHE_SOCSCI or 
    T.UM_BHE_SCI <> S.UM_BHE_SCI or 
    T.UM_BHE_MATH <> S.UM_BHE_MATH or 
    T.UM_BHE_ELT <> S.UM_BHE_ELT or 
    T.UM_BHE_FRLG <> S.UM_BHE_FRLG or 
    T.UM_BHE_CMPLT <> S.UM_BHE_CMPLT or 
    T.UM_BHE_EXP_VOCTEC <> S.UM_BHE_EXP_VOCTEC or 
    T.UM_BHE_EXP_ESL <> S.UM_BHE_EXP_ESL or 
    T.UM_BHE_EXP_INTL <> S.UM_BHE_EXP_INTL or 
    T.UM_BHE_PRECOLLEGE <> S.UM_BHE_PRECOLLEGE or 
    T.UM_BHE_EXP_LD <> S.UM_BHE_EXP_LD or 
    T.UM_BHE_TRANS_CR <> S.UM_BHE_TRANS_CR or 
    T.UM_BHE_TRANS_GPA <> S.UM_BHE_TRANS_GPA or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.APPL_PROG_NBR,
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.UM_BHE, 
    T.UM_BHE_ENG, 
    T.UM_BHE_SOCSCI,
    T.UM_BHE_SCI, 
    T.UM_BHE_MATH,
    T.UM_BHE_ELT, 
    T.UM_BHE_FRLG,
    T.UM_BHE_CMPLT, 
    T.UM_BHE_EXP_VOCTEC,
    T.UM_BHE_EXP_ESL, 
    T.UM_BHE_EXP_INTL,
    T.UM_BHE_PRECOLLEGE,
    T.UM_BHE_EXP_LD,
    T.UM_BHE_TRANS_CR,
    T.UM_BHE_TRANS_GPA, 
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
    S.INSTITUTION,
    S.UM_BHE, 
    S.UM_BHE_ENG, 
    S.UM_BHE_SOCSCI,
    S.UM_BHE_SCI, 
    S.UM_BHE_MATH,
    S.UM_BHE_ELT, 
    S.UM_BHE_FRLG,
    S.UM_BHE_CMPLT, 
    S.UM_BHE_EXP_VOCTEC,
    S.UM_BHE_EXP_ESL, 
    S.UM_BHE_EXP_INTL,
    S.UM_BHE_PRECOLLEGE,
    S.UM_BHE_EXP_LD,
    S.UM_BHE_TRANS_CR,
    S.UM_BHE_TRANS_GPA, 
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

strMessage01    := '# of PS_UM_ADM_BHE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_BHE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_ADM_BHE';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_BHE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_BHE';
update CSSTG_OWNER.PS_UM_ADM_BHE T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR
   from CSSTG_OWNER.PS_UM_ADM_BHE T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_BHE') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR
   from SYSADM.PS_UM_ADM_BHE@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_BHE') = 'Y' 
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

strMessage01    := '# of PS_UM_ADM_BHE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_BHE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_ADM_BHE'
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

END PS_UM_ADM_BHE_P;
/

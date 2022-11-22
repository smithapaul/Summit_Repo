DROP PROCEDURE CSMRT_OWNER.PS_TRNS_TEST_TERM_P
/

--
-- PS_TRNS_TEST_TERM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_TRNS_TEST_TERM_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNS_TEST_TERM from PeopleSoft table PS_TRNS_TEST_TERM.
--
 --V01  SMT-xxxx 10/05/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TRNS_TEST_TERM';
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
 where TABLE_NAME = 'PS_TRNS_TEST_TERM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNS_TEST_TERM@SASOURCE S)
 where TABLE_NAME = 'PS_TRNS_TEST_TERM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TRNS_TEST_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TRNS_TEST_TERM';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TRNS_TEST_TERM T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ARTICULATION_TERM),'-') ARTICULATION_TERM, 
    nvl(trim(MODEL_STATUS),'-') MODEL_STATUS, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(TRF_PASSED_GPA,0) TRF_PASSED_GPA, 
    nvl(TRF_PASSED_NOGPA,0) TRF_PASSED_NOGPA, 
    NVL(POST_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) POST_DT, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(TRF_TAKEN_GPA,0) TRF_TAKEN_GPA, 
    nvl(TRF_TAKEN_NOGPA,0) TRF_TAKEN_NOGPA, 
    nvl(TRF_GRADE_POINTS,0) TRF_GRADE_POINTS, 
    nvl(TRF_GPA,0) TRF_GPA, 
    nvl(SSR_FAWI_PSD,0) SSR_FAWI_PSD, 
    nvl(SSR_FAWI_TKN_GPA,0) SSR_FAWI_TKN_GPA, 
    nvl(SSR_FAWI_TKN_NOGPA,0) SSR_FAWI_TKN_NOGPA, 
    nvl(SSR_FAWI_PSD_GPA,0) SSR_FAWI_PSD_GPA, 
    nvl(SSR_FAWI_PSD_NOGPA,0) SSR_FAWI_PSD_NOGPA, 
    nvl(SSR_FAWI_GRADE_PTS,0) SSR_FAWI_GRADE_PTS, 
    nvl(SSR_FAWI_GPA,0) SSR_FAWI_GPA
from SYSADM.PS_TRNS_TEST_TERM@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_TERM') ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.MODEL_NBR = S.MODEL_NBR and 
    T.ARTICULATION_TERM = S.ARTICULATION_TERM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.MODEL_STATUS = S.MODEL_STATUS,
    T.UNT_TRNSFR = S.UNT_TRNSFR,
    T.TRF_PASSED_GPA = S.TRF_PASSED_GPA,
    T.TRF_PASSED_NOGPA = S.TRF_PASSED_NOGPA,
    T.POST_DT = S.POST_DT,
    T.OPRID = S.OPRID,
    T.TRF_TAKEN_GPA = S.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA = S.TRF_TAKEN_NOGPA,
    T.TRF_GRADE_POINTS = S.TRF_GRADE_POINTS,
    T.TRF_GPA = S.TRF_GPA,
    T.SSR_FAWI_PSD = S.SSR_FAWI_PSD,
    T.SSR_FAWI_TKN_GPA = S.SSR_FAWI_TKN_GPA,
    T.SSR_FAWI_TKN_NOGPA = S.SSR_FAWI_TKN_NOGPA,
    T.SSR_FAWI_PSD_GPA = S.SSR_FAWI_PSD_GPA,
    T.SSR_FAWI_PSD_NOGPA = S.SSR_FAWI_PSD_NOGPA,
    T.SSR_FAWI_GRADE_PTS = S.SSR_FAWI_GRADE_PTS,
    T.SSR_FAWI_GPA = S.SSR_FAWI_GPA,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.MODEL_STATUS <> S.MODEL_STATUS or 
    T.UNT_TRNSFR <> S.UNT_TRNSFR or 
    T.TRF_PASSED_GPA <> S.TRF_PASSED_GPA or 
    T.TRF_PASSED_NOGPA <> S.TRF_PASSED_NOGPA or 
    nvl(trim(T.POST_DT),0) <> nvl(trim(S.POST_DT),0) or 
    T.OPRID <> S.OPRID or 
    T.TRF_TAKEN_GPA <> S.TRF_TAKEN_GPA or 
    T.TRF_TAKEN_NOGPA <> S.TRF_TAKEN_NOGPA or 
    T.TRF_GRADE_POINTS <> S.TRF_GRADE_POINTS or 
    T.TRF_GPA <> S.TRF_GPA or 
    T.SSR_FAWI_PSD <> S.SSR_FAWI_PSD or 
    T.SSR_FAWI_TKN_GPA <> S.SSR_FAWI_TKN_GPA or 
    T.SSR_FAWI_TKN_NOGPA <> S.SSR_FAWI_TKN_NOGPA or 
    T.SSR_FAWI_PSD_GPA <> S.SSR_FAWI_PSD_GPA or 
    T.SSR_FAWI_PSD_NOGPA <> S.SSR_FAWI_PSD_NOGPA or 
    T.SSR_FAWI_GRADE_PTS <> S.SSR_FAWI_GRADE_PTS or 
    T.SSR_FAWI_GPA <> S.SSR_FAWI_GPA or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.MODEL_NBR,
    T.ARTICULATION_TERM,
    T.SRC_SYS_ID, 
    T.MODEL_STATUS, 
    T.UNT_TRNSFR, 
    T.TRF_PASSED_GPA, 
    T.TRF_PASSED_NOGPA, 
    T.POST_DT,
    T.OPRID,
    T.TRF_TAKEN_GPA,
    T.TRF_TAKEN_NOGPA,
    T.TRF_GRADE_POINTS, 
    T.TRF_GPA,
    T.SSR_FAWI_PSD, 
    T.SSR_FAWI_TKN_GPA, 
    T.SSR_FAWI_TKN_NOGPA, 
    T.SSR_FAWI_PSD_GPA, 
    T.SSR_FAWI_PSD_NOGPA, 
    T.SSR_FAWI_GRADE_PTS, 
    T.SSR_FAWI_GPA, 
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
    S.MODEL_NBR,
    S.ARTICULATION_TERM,
    'CS90', 
    S.MODEL_STATUS, 
    S.UNT_TRNSFR, 
    S.TRF_PASSED_GPA, 
    S.TRF_PASSED_NOGPA, 
    S.POST_DT,
    S.OPRID,
    S.TRF_TAKEN_GPA,
    S.TRF_TAKEN_NOGPA,
    S.TRF_GRADE_POINTS, 
    S.TRF_GPA,
    S.SSR_FAWI_PSD, 
    S.SSR_FAWI_TKN_GPA, 
    S.SSR_FAWI_TKN_NOGPA, 
    S.SSR_FAWI_PSD_GPA, 
    S.SSR_FAWI_PSD_NOGPA, 
    S.SSR_FAWI_GRADE_PTS, 
    S.SSR_FAWI_GPA, 
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

strMessage01    := '# of PS_TRNS_TEST_TERM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_TERM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNS_TEST_TERM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_TEST_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_TEST_TERM';
update CSSTG_OWNER.PS_TRNS_TEST_TERM T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM
   from CSSTG_OWNER.PS_TRNS_TEST_TERM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_TERM') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM
   from SYSADM.PS_TRNS_TEST_TERM@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_TERM') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.MODEL_NBR = S.MODEL_NBR
   and T.ARTICULATION_TERM = S.ARTICULATION_TERM
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNS_TEST_TERM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_TERM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNS_TEST_TERM'
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

END PS_TRNS_TEST_TERM_P;
/

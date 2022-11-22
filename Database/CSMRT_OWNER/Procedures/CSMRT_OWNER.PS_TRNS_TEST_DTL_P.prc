DROP PROCEDURE CSMRT_OWNER.PS_TRNS_TEST_DTL_P
/

--
-- PS_TRNS_TEST_DTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_TRNS_TEST_DTL_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNS_TEST_DTL from PeopleSoft table PS_TRNS_TEST_DTL.
--
-- V01  SMT-xxxx 9/29/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TRNS_TEST_DTL';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_TRNS_TEST_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNS_TEST_DTL@SASOURCE S)
 where TABLE_NAME = 'PS_TRNS_TEST_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_TRNS_TEST_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_TRNS_TEST_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_TRNS_TEST_DTL
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ARTICULATION_TERM),'-') ARTICULATION_TERM, 
    nvl(TRNSFR_EQVLNCY_GRP,0) TRNSFR_EQVLNCY_GRP, 
    nvl(TRNSFR_EQVLNCY_SEQ,0) TRNSFR_EQVLNCY_SEQ, 
    nvl(trim(TRNSFR_STAT),'-') TRNSFR_STAT, 
    nvl(trim(TST_EQVLNCY),'-') TST_EQVLNCY, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    nvl(trim(TEST_ID),'-') TEST_ID, 
    nvl(trim(TEST_COMPONENT),'-') TEST_COMPONENT, 
    nvl(trim(DESCR),'-') DESCR, 
    TEST_DT, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(SCORE,0) SCORE, 
    nvl(PERCENTILE,0) PERCENTILE, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(trim(CRSE_GRADE_OFF),'-') CRSE_GRADE_OFF, 
    nvl(GRD_PTS_PER_UNIT,0) GRD_PTS_PER_UNIT, 
    nvl(trim(EARN_CREDIT),'-') EARN_CREDIT, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    nvl(trim(UNITS_ATTEMPTED),'-') UNITS_ATTEMPTED, 
    nvl(trim(REPEAT_CODE),'-') REPEAT_CODE, 
    nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN, 
    nvl(trim(FREEZE_REC_FL),'-') FREEZE_REC_FL, 
    nvl(trim(INPUT_CHG_FL),'-') INPUT_CHG_FL, 
    nvl(trim(REJECT_REASON),'-') REJECT_REASON, 
    nvl(trim(OVRD_TRCR_FL),'-') OVRD_TRCR_FL, 
    nvl(trim(OVRD_RSN),'-') OVRD_RSN, 
    nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT, 
    nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY, 
    nvl(trim(SSR_FAWI_INCL),'-') SSR_FAWI_INCL, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_TRNS_TEST_DTL@SASOURCE S 
where EMPLID between '00000000' and '99999999'
 and length(EMPLID) = 8; 

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_TRNS_TEST_DTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TRNS_TEST_DTL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TRNS_TEST_DTL T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ARTICULATION_TERM),'-') ARTICULATION_TERM, 
    nvl(TRNSFR_EQVLNCY_GRP,0) TRNSFR_EQVLNCY_GRP, 
    nvl(TRNSFR_EQVLNCY_SEQ,0) TRNSFR_EQVLNCY_SEQ, 
    nvl(trim(TRNSFR_STAT),'-') TRNSFR_STAT, 
    nvl(trim(TST_EQVLNCY),'-') TST_EQVLNCY, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    nvl(trim(TEST_ID),'-') TEST_ID, 
    nvl(trim(TEST_COMPONENT),'-') TEST_COMPONENT, 
    nvl(trim(DESCR),'-') DESCR, 
    NVL(TEST_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) TEST_DT, 
    nvl(trim(LS_DATA_SOURCE),'-') LS_DATA_SOURCE, 
    nvl(SCORE,0) SCORE, 
    nvl(PERCENTILE,0) PERCENTILE, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(trim(CRSE_GRADE_OFF),'-') CRSE_GRADE_OFF, 
    nvl(GRD_PTS_PER_UNIT,0) GRD_PTS_PER_UNIT, 
    nvl(trim(EARN_CREDIT),'-') EARN_CREDIT, 
    nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA, 
    nvl(trim(UNITS_ATTEMPTED),'-') UNITS_ATTEMPTED, 
    nvl(trim(REPEAT_CODE),'-') REPEAT_CODE, 
    nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN, 
    nvl(trim(FREEZE_REC_FL),'-') FREEZE_REC_FL, 
    nvl(trim(INPUT_CHG_FL),'-') INPUT_CHG_FL, 
    nvl(trim(REJECT_REASON),'-') REJECT_REASON, 
    nvl(trim(OVRD_TRCR_FL),'-') OVRD_TRCR_FL, 
    nvl(trim(OVRD_RSN),'-') OVRD_RSN, 
    nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT, 
    nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY, 
    nvl(trim(SSR_FAWI_INCL),'-') SSR_FAWI_INCL, 
    COMMENTS COMMENTS
from CSSTG_OWNER.PS_T_TRNS_TEST_DTL S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_DTL') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.MODEL_NBR = S.MODEL_NBR and 
    T.ARTICULATION_TERM = S.ARTICULATION_TERM and 
    T.TRNSFR_EQVLNCY_GRP = S.TRNSFR_EQVLNCY_GRP and 
    T.TRNSFR_EQVLNCY_SEQ = S.TRNSFR_EQVLNCY_SEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.TRNSFR_STAT = S.TRNSFR_STAT,
    T.TST_EQVLNCY = S.TST_EQVLNCY,
    T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP,
    T.TEST_ID = S.TEST_ID,
    T.TEST_COMPONENT = S.TEST_COMPONENT,
    T.DESCR = S.DESCR,
    T.TEST_DT = S.TEST_DT,
    T.LS_DATA_SOURCE = S.LS_DATA_SOURCE,
    T.SCORE = S.SCORE,
    T.PERCENTILE = S.PERCENTILE,
    T.CRSE_ID = S.CRSE_ID,
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR,
    T.UNT_TRNSFR = S.UNT_TRNSFR,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS = S.GRADING_BASIS,
    T.CRSE_GRADE_OFF = S.CRSE_GRADE_OFF,
    T.GRD_PTS_PER_UNIT = S.GRD_PTS_PER_UNIT,
    T.EARN_CREDIT = S.EARN_CREDIT,
    T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
    T.UNITS_ATTEMPTED = S.UNITS_ATTEMPTED,
    T.REPEAT_CODE = S.REPEAT_CODE,
    T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
    T.FREEZE_REC_FL = S.FREEZE_REC_FL,
    T.INPUT_CHG_FL = S.INPUT_CHG_FL,
    T.REJECT_REASON = S.REJECT_REASON,
    T.OVRD_TRCR_FL = S.OVRD_TRCR_FL,
    T.OVRD_RSN = S.OVRD_RSN,
    T.VALID_ATTEMPT = S.VALID_ATTEMPT,
    T.GRADE_CATEGORY = S.GRADE_CATEGORY,
    T.SSR_FAWI_INCL = S.SSR_FAWI_INCL,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.TRNSFR_STAT <> S.TRNSFR_STAT or 
    T.TST_EQVLNCY <> S.TST_EQVLNCY or 
    T.TRNSFR_EQVLNCY_CMP <> S.TRNSFR_EQVLNCY_CMP or 
    T.TEST_ID <> S.TEST_ID or 
    T.TEST_COMPONENT <> S.TEST_COMPONENT or 
    T.DESCR <> S.DESCR or 
    nvl(trim(T.TEST_DT),0) <> nvl(trim(S.TEST_DT),0) or 
    T.LS_DATA_SOURCE <> S.LS_DATA_SOURCE or 
    T.SCORE <> S.SCORE or 
    T.PERCENTILE <> S.PERCENTILE or 
    T.CRSE_ID <> S.CRSE_ID or 
    T.CRSE_OFFER_NBR <> S.CRSE_OFFER_NBR or 
    T.UNT_TRNSFR <> S.UNT_TRNSFR or 
    T.GRADING_SCHEME <> S.GRADING_SCHEME or 
    T.GRADING_BASIS <> S.GRADING_BASIS or 
    T.CRSE_GRADE_OFF <> S.CRSE_GRADE_OFF or 
    T.GRD_PTS_PER_UNIT <> S.GRD_PTS_PER_UNIT or 
    T.EARN_CREDIT <> S.EARN_CREDIT or 
    T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA or 
    T.UNITS_ATTEMPTED <> S.UNITS_ATTEMPTED or 
    T.REPEAT_CODE <> S.REPEAT_CODE or 
    T.RQMNT_DESIGNTN <> S.RQMNT_DESIGNTN or 
    T.FREEZE_REC_FL <> S.FREEZE_REC_FL or 
    T.INPUT_CHG_FL <> S.INPUT_CHG_FL or 
    T.REJECT_REASON <> S.REJECT_REASON or 
    T.OVRD_TRCR_FL <> S.OVRD_TRCR_FL or 
    T.OVRD_RSN <> S.OVRD_RSN or 
    T.VALID_ATTEMPT <> S.VALID_ATTEMPT or 
    T.GRADE_CATEGORY <> S.GRADE_CATEGORY or 
    T.SSR_FAWI_INCL <> S.SSR_FAWI_INCL or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.MODEL_NBR,
    T.ARTICULATION_TERM,
    T.TRNSFR_EQVLNCY_GRP, 
    T.TRNSFR_EQVLNCY_SEQ, 
    T.SRC_SYS_ID, 
    T.TRNSFR_STAT,
    T.TST_EQVLNCY,
    T.TRNSFR_EQVLNCY_CMP, 
    T.TEST_ID,
    T.TEST_COMPONENT, 
    T.DESCR,
    T.TEST_DT,
    T.LS_DATA_SOURCE, 
    T.SCORE,
    T.PERCENTILE, 
    T.CRSE_ID,
    T.CRSE_OFFER_NBR, 
    T.UNT_TRNSFR, 
    T.GRADING_SCHEME, 
    T.GRADING_BASIS,
    T.CRSE_GRADE_OFF, 
    T.GRD_PTS_PER_UNIT, 
    T.EARN_CREDIT,
    T.INCLUDE_IN_GPA, 
    T.UNITS_ATTEMPTED,
    T.REPEAT_CODE,
    T.RQMNT_DESIGNTN, 
    T.FREEZE_REC_FL,
    T.INPUT_CHG_FL, 
    T.REJECT_REASON,
    T.OVRD_TRCR_FL, 
    T.OVRD_RSN, 
    T.VALID_ATTEMPT,
    T.GRADE_CATEGORY, 
    T.SSR_FAWI_INCL,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.MODEL_NBR,
    S.ARTICULATION_TERM,
    S.TRNSFR_EQVLNCY_GRP, 
    S.TRNSFR_EQVLNCY_SEQ, 
    'CS90', 
    S.TRNSFR_STAT,
    S.TST_EQVLNCY,
    S.TRNSFR_EQVLNCY_CMP, 
    S.TEST_ID,
    S.TEST_COMPONENT, 
    S.DESCR,
    S.TEST_DT,
    S.LS_DATA_SOURCE, 
    S.SCORE,
    S.PERCENTILE, 
    S.CRSE_ID,
    S.CRSE_OFFER_NBR, 
    S.UNT_TRNSFR, 
    S.GRADING_SCHEME, 
    S.GRADING_BASIS,
    S.CRSE_GRADE_OFF, 
    S.GRD_PTS_PER_UNIT, 
    S.EARN_CREDIT,
    S.INCLUDE_IN_GPA, 
    S.UNITS_ATTEMPTED,
    S.REPEAT_CODE,
    S.RQMNT_DESIGNTN, 
    S.FREEZE_REC_FL,
    S.INPUT_CHG_FL, 
    S.REJECT_REASON,
    S.OVRD_TRCR_FL, 
    S.OVRD_RSN, 
    S.VALID_ATTEMPT,
    S.GRADE_CATEGORY, 
    S.SSR_FAWI_INCL,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_TRNS_TEST_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNS_TEST_DTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TRNS_TEST_DTL';
update CSSTG_OWNER.PS_TRNS_TEST_DTL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ
   from CSSTG_OWNER.PS_TRNS_TEST_DTL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_DTL') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ
   from SYSADM.PS_TRNS_TEST_DTL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_TEST_DTL') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.MODEL_NBR = S.MODEL_NBR
   and T.ARTICULATION_TERM = S.ARTICULATION_TERM
   and T.TRNSFR_EQVLNCY_GRP = S.TRNSFR_EQVLNCY_GRP
   and T.TRNSFR_EQVLNCY_SEQ = S.TRNSFR_EQVLNCY_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNS_TEST_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_TEST_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNS_TEST_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END PS_TRNS_TEST_DTL_P;
/

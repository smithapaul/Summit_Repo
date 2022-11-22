DROP PROCEDURE CSMRT_OWNER.AM_PS_TRNS_CRSE_DTL_P
/

--
-- AM_PS_TRNS_CRSE_DTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_TRNS_CRSE_DTL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNS_CRSE_DTL from PeopleSoft table PS_TRNS_CRSE_DTL.
--
-- V01  SMT-xxxx 9/28/2017,    James Doucette
--                             Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TRNS_CRSE_DTL';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNS_CRSE_DTL@AMSOURCE S)
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_TRNS_CRSE_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_TRNS_CRSE_DTL
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ARTICULATION_TERM),'-') ARTICULATION_TERM, 
    nvl(TRNSFR_EQVLNCY_GRP,0) TRNSFR_EQVLNCY_GRP, 
    nvl(TRNSFR_EQVLNCY_SEQ,0) TRNSFR_EQVLNCY_SEQ, 
    nvl(trim(TRNSFR_STAT),'-') TRNSFR_STAT, 
    nvl(trim(TRNSFR_SRC_ID),'-') TRNSFR_SRC_ID, 
    nvl(trim(TRNSFR_EQVLNCY),'-') TRNSFR_EQVLNCY, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    nvl(EXT_COURSE_NBR,0) EXT_COURSE_NBR, 
    nvl(trim(SRC_TERM),'-') SRC_TERM, 
    nvl(SRC_CLASS_NBR,0) SRC_CLASS_NBR, 
    nvl(TERM_YEAR,0) TERM_YEAR, 
    nvl(trim(EXT_TERM),'-') EXT_TERM, 
    replace(nvl(trim(SCHOOL_SUBJECT),'-'), '  ', ' ') SCHOOL_SUBJECT, 
    nvl(trim(SCHOOL_CRSE_NBR),'-') SCHOOL_CRSE_NBR, 
    replace(nvl(trim(DESCR),'-'),'  ', ' ') DESCR, 
    nvl(SSR_UNT_TAKEN_EXT,0) SSR_UNT_TAKEN_EXT, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
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
    nvl(trim(COURSE_LEVEL),'-') COURSE_LEVEL, 
    nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT, 
    nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY, 
    nvl(trim(COMP_SUBJECT_AREA),'-') COMP_SUBJECT_AREA, 
    nvl(trim(SSR_FAWI_INCL),'-') SSR_FAWI_INCL, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_TRNS_CRSE_DTL@AMSOURCE S
where EMPLID between '00000000' and '99999999'
 and length(EMPLID) = 8;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TRNS_CRSE_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TRNS_CRSE_DTL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TRNS_CRSE_DTL T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(MODEL_NBR,0) MODEL_NBR, 
    nvl(trim(ARTICULATION_TERM),'-') ARTICULATION_TERM, 
    nvl(TRNSFR_EQVLNCY_GRP,0) TRNSFR_EQVLNCY_GRP, 
    nvl(TRNSFR_EQVLNCY_SEQ,0) TRNSFR_EQVLNCY_SEQ, 
    nvl(trim(TRNSFR_STAT),'-') TRNSFR_STAT, 
    nvl(trim(TRNSFR_SRC_ID),'-') TRNSFR_SRC_ID, 
    nvl(trim(TRNSFR_EQVLNCY),'-') TRNSFR_EQVLNCY, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    nvl(EXT_COURSE_NBR,0) EXT_COURSE_NBR, 
    nvl(trim(SRC_TERM),'-') SRC_TERM, 
    nvl(SRC_CLASS_NBR,0) SRC_CLASS_NBR, 
    nvl(TERM_YEAR,0) TERM_YEAR, 
    nvl(trim(EXT_TERM),'-') EXT_TERM, 
    nvl(trim(SCHOOL_SUBJECT),'-') SCHOOL_SUBJECT, 
    nvl(trim(SCHOOL_CRSE_NBR),'-') SCHOOL_CRSE_NBR, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(SSR_UNT_TAKEN_EXT,0) SSR_UNT_TAKEN_EXT, 
    nvl(UNT_TAKEN,0) UNT_TAKEN, 
    nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT, 
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(UNT_TRNSFR,0) UNT_TRNSFR, 
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
    nvl(trim(COURSE_LEVEL),'-') COURSE_LEVEL, 
    nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT, 
    nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY, 
    nvl(trim(COMP_SUBJECT_AREA),'-') COMP_SUBJECT_AREA, 
    nvl(trim(SSR_FAWI_INCL),'-') SSR_FAWI_INCL, 
    COMMENTS COMMENTS
from AMSTG_OWNER.PS_T_TRNS_CRSE_DTL S 
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_DTL') 
 and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 
) S
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
    T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID,
    T.TRNSFR_EQVLNCY = S.TRNSFR_EQVLNCY,
    T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP,
    T.EXT_COURSE_NBR = S.EXT_COURSE_NBR,
    T.SRC_TERM = S.SRC_TERM,
    T.SRC_CLASS_NBR = S.SRC_CLASS_NBR,
    T.TERM_YEAR = S.TERM_YEAR,
    T.EXT_TERM = S.EXT_TERM,
    T.SCHOOL_SUBJECT = S.SCHOOL_SUBJECT,
    T.SCHOOL_CRSE_NBR = S.SCHOOL_CRSE_NBR,
    T.DESCR = S.DESCR,
    T.SSR_UNT_TAKEN_EXT = S.SSR_UNT_TAKEN_EXT,
    T.UNT_TAKEN = S.UNT_TAKEN,
    T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT,
    T.CRSE_ID = S.CRSE_ID,
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS = S.GRADING_BASIS,
    T.UNT_TRNSFR = S.UNT_TRNSFR,
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
    T.COURSE_LEVEL = S.COURSE_LEVEL,
    T.VALID_ATTEMPT = S.VALID_ATTEMPT,
    T.GRADE_CATEGORY = S.GRADE_CATEGORY,
    T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA,
    T.SSR_FAWI_INCL = S.SSR_FAWI_INCL,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.TRNSFR_STAT <> S.TRNSFR_STAT or 
    T.TRNSFR_SRC_ID <> S.TRNSFR_SRC_ID or 
    T.TRNSFR_EQVLNCY <> S.TRNSFR_EQVLNCY or 
    T.TRNSFR_EQVLNCY_CMP <> S.TRNSFR_EQVLNCY_CMP or 
    T.EXT_COURSE_NBR <> S.EXT_COURSE_NBR or 
    T.SRC_TERM <> S.SRC_TERM or 
    T.SRC_CLASS_NBR <> S.SRC_CLASS_NBR or 
    T.TERM_YEAR <> S.TERM_YEAR or 
    T.EXT_TERM <> S.EXT_TERM or 
    T.SCHOOL_SUBJECT <> S.SCHOOL_SUBJECT or 
    T.SCHOOL_CRSE_NBR <> S.SCHOOL_CRSE_NBR or 
    T.DESCR <> S.DESCR or 
    T.SSR_UNT_TAKEN_EXT <> S.SSR_UNT_TAKEN_EXT or 
    T.UNT_TAKEN <> S.UNT_TAKEN or 
    T.CRSE_GRADE_INPUT <> S.CRSE_GRADE_INPUT or 
    T.CRSE_ID <> S.CRSE_ID or 
    T.CRSE_OFFER_NBR <> S.CRSE_OFFER_NBR or 
    T.GRADING_SCHEME <> S.GRADING_SCHEME or 
    T.GRADING_BASIS <> S.GRADING_BASIS or 
    T.UNT_TRNSFR <> S.UNT_TRNSFR or 
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
    T.COURSE_LEVEL <> S.COURSE_LEVEL or 
    T.VALID_ATTEMPT <> S.VALID_ATTEMPT or 
    T.GRADE_CATEGORY <> S.GRADE_CATEGORY or 
    T.COMP_SUBJECT_AREA <> S.COMP_SUBJECT_AREA or 
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
    T.TRNSFR_SRC_ID,
    T.TRNSFR_EQVLNCY, 
    T.TRNSFR_EQVLNCY_CMP, 
    T.EXT_COURSE_NBR, 
    T.SRC_TERM, 
    T.SRC_CLASS_NBR,
    T.TERM_YEAR,
    T.EXT_TERM, 
    T.SCHOOL_SUBJECT, 
    T.SCHOOL_CRSE_NBR,
    T.DESCR,
    T.SSR_UNT_TAKEN_EXT,
    T.UNT_TAKEN,
    T.CRSE_GRADE_INPUT, 
    T.CRSE_ID,
    T.CRSE_OFFER_NBR, 
    T.GRADING_SCHEME, 
    T.GRADING_BASIS,
    T.UNT_TRNSFR, 
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
    T.COURSE_LEVEL, 
    T.VALID_ATTEMPT,
    T.GRADE_CATEGORY, 
    T.COMP_SUBJECT_AREA,
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
    S.TRNSFR_SRC_ID,
    S.TRNSFR_EQVLNCY, 
    S.TRNSFR_EQVLNCY_CMP, 
    S.EXT_COURSE_NBR, 
    S.SRC_TERM, 
    S.SRC_CLASS_NBR,
    S.TERM_YEAR,
    S.EXT_TERM, 
    S.SCHOOL_SUBJECT, 
    S.SCHOOL_CRSE_NBR,
    S.DESCR,
    S.SSR_UNT_TAKEN_EXT,
    S.UNT_TAKEN,
    S.CRSE_GRADE_INPUT, 
    S.CRSE_ID,
    S.CRSE_OFFER_NBR, 
    S.GRADING_SCHEME, 
    S.GRADING_BASIS,
    S.UNT_TRNSFR, 
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
    S.COURSE_LEVEL, 
    S.VALID_ATTEMPT,
    S.GRADE_CATEGORY, 
    S.COMP_SUBJECT_AREA,
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


strMessage01    := '# of PS_TRNS_CRSE_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_CRSE_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TRNS_CRSE_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TRNS_CRSE_DTL';
update AMSTG_OWNER.PS_TRNS_CRSE_DTL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ
   from AMSTG_OWNER.PS_TRNS_CRSE_DTL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_DTL') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ
   from SYSADM.PS_TRNS_CRSE_DTL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNS_CRSE_DTL') = 'Y'
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

strMessage01    := '# of PS_TRNS_CRSE_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNS_CRSE_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNS_CRSE_DTL'
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

END AM_PS_TRNS_CRSE_DTL_P;
/

DROP PROCEDURE CSMRT_OWNER.PS_STDNT_TERM_BDGT_P
/

--
-- PS_STDNT_TERM_BDGT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_STDNT_TERM_BDGT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_TERM_BDGT from PeopleSoft table PS_STDNT_TERM_BDGT.
--
-- V01  SMT-xxxx 04/18/2017,    Jim Doucette
--                              Converted from PS_STDNT_TERM_BDGT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_STDNT_TERM_BDGT';
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
 where TABLE_NAME = 'PS_STDNT_TERM_BDGT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_TERM_BDGT@SASOURCE S)
 where TABLE_NAME = 'PS_STDNT_TERM_BDGT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_STDNT_TERM_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_STDNT_TERM_BDGT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_STDNT_TERM_BDGT T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(STRM),'-') STRM,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(EFFSEQ,0) EFFSEQ,
nvl(trim(BUDGET_GROUP_CODE),'-') BUDGET_GROUP_CODE,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(OPRID),'-') OPRID,
nvl(trim(TERM_TYPE),'-') TERM_TYPE,
nvl(trim(ACAD_PROG_PRIMARY),'-') ACAD_PROG_PRIMARY,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN,
nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD,
nvl(trim(ACAD_LOAD_APPR),'-') ACAD_LOAD_APPR,
nvl(trim(ACAD_LEVEL_PROJ),'-') ACAD_LEVEL_PROJ,
nvl(trim(ACAD_LEVEL_BOT),'-') ACAD_LEVEL_BOT,
nvl(trim(FA_LOAD),'-') FA_LOAD,
nvl(trim(FORM_OF_STUDY),'-') FORM_OF_STUDY,
nvl(trim(RESIDENCY),'-') RESIDENCY,
nvl(trim(FIN_AID_FED_RES),'-') FIN_AID_FED_RES,
nvl(trim(FIN_AID_ST_RES),'-') FIN_AID_ST_RES,
nvl(trim(FIN_AID_FED_EXCPT),'-') FIN_AID_FED_EXCPT,
nvl(trim(FIN_AID_ST_EXCPT),'-') FIN_AID_ST_EXCPT,
nvl(trim(STATE_RESIDENCE),'-') STATE_RESIDENCE,
nvl(trim(APP_STATE_RESIDENC),'-') APP_STATE_RESIDENC,
nvl(trim(HOUSING_TYPE),'-') HOUSING_TYPE,
nvl(FED_TERM_COA,0) FED_TERM_COA,
nvl(INST_TERM_COA,0) INST_TERM_COA,
nvl(PELL_TERM_COA,0) PELL_TERM_COA,
nvl(SFA_PELTRM_COA_LHT,0) SFA_PELTRM_COA_LHT,
nvl(trim(MARITAL_STAT),'-') MARITAL_STAT,
nvl(trim(NUMBER_IN_FAMILY),'-') NUMBER_IN_FAMILY,
nvl(trim(DEPNDNCY_STAT),'-') DEPNDNCY_STAT,
nvl(trim(NSLDS_LOAN_YEAR),'-') NSLDS_LOAN_YEAR,
nvl(trim(PRORATE_BUDGET),'-') PRORATE_BUDGET,
nvl(trim(POSTAL),'-') POSTAL,
nvl(FA_UNIT_ANTIC,0) FA_UNIT_ANTIC,
nvl(FA_UNIT_COMPLETED,0) FA_UNIT_COMPLETED,
nvl(FA_UNIT_IN_PROG,0) FA_UNIT_IN_PROG,
nvl(FA_UNIT_CURRENT,0) FA_UNIT_CURRENT,
nvl(trim(ACAD_PLAN_TYPE),'-') ACAD_PLAN_TYPE,
to_date(to_char(case when FA_TERM_EFFDT < '01-JAN-1800' then NULL else FA_TERM_EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') FA_TERM_EFFDT,
nvl(FA_TERM_EFFSEQ,0) FA_TERM_EFFSEQ,
nvl(trim(FA_LOAD_CURRENT),'-') FA_LOAD_CURRENT
from SYSADM.PS_STDNT_TERM_BDGT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TERM_BDGT') 
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ACAD_CAREER = S.ACAD_CAREER and
T.STRM = S.STRM and
T.EFFDT = S.EFFDT and
T.EFFSEQ = S.EFFSEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.BUDGET_GROUP_CODE = S.BUDGET_GROUP_CODE,
T.DESCRSHORT = S.DESCRSHORT,
T.OPRID = S.OPRID,
T.TERM_TYPE = S.TERM_TYPE,
T.ACAD_PROG_PRIMARY = S.ACAD_PROG_PRIMARY,
T.ACAD_PLAN = S.ACAD_PLAN,
T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN,
T.ACADEMIC_LOAD = S.ACADEMIC_LOAD,
T.ACAD_LOAD_APPR = S.ACAD_LOAD_APPR,
T.ACAD_LEVEL_PROJ = S.ACAD_LEVEL_PROJ,
T.ACAD_LEVEL_BOT = S.ACAD_LEVEL_BOT,
T.FA_LOAD = S.FA_LOAD,
T.FORM_OF_STUDY = S.FORM_OF_STUDY,
T.RESIDENCY = S.RESIDENCY,
T.FIN_AID_FED_RES = S.FIN_AID_FED_RES,
T.FIN_AID_ST_RES = S.FIN_AID_ST_RES,
T.FIN_AID_FED_EXCPT = S.FIN_AID_FED_EXCPT,
T.FIN_AID_ST_EXCPT = S.FIN_AID_ST_EXCPT,
T.STATE_RESIDENCE = S.STATE_RESIDENCE,
T.APP_STATE_RESIDENC = S.APP_STATE_RESIDENC,
T.HOUSING_TYPE = S.HOUSING_TYPE,
T.FED_TERM_COA = S.FED_TERM_COA,
T.INST_TERM_COA = S.INST_TERM_COA,
T.PELL_TERM_COA = S.PELL_TERM_COA,
T.SFA_PELTRM_COA_LHT = S.SFA_PELTRM_COA_LHT,
T.MARITAL_STAT = S.MARITAL_STAT,
T.NUMBER_IN_FAMILY = S.NUMBER_IN_FAMILY,
T.DEPNDNCY_STAT = S.DEPNDNCY_STAT,
T.NSLDS_LOAN_YEAR = S.NSLDS_LOAN_YEAR,
T.PRORATE_BUDGET = S.PRORATE_BUDGET,
T.POSTAL = S.POSTAL,
T.FA_UNIT_ANTIC = S.FA_UNIT_ANTIC,
T.FA_UNIT_COMPLETED = S.FA_UNIT_COMPLETED,
T.FA_UNIT_IN_PROG = S.FA_UNIT_IN_PROG,
T.FA_UNIT_CURRENT = S.FA_UNIT_CURRENT,
T.ACAD_PLAN_TYPE = S.ACAD_PLAN_TYPE,
T.FA_TERM_EFFDT = S.FA_TERM_EFFDT,
T.FA_TERM_EFFSEQ = S.FA_TERM_EFFSEQ,
T.FA_LOAD_CURRENT = S.FA_LOAD_CURRENT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.BUDGET_GROUP_CODE <> S.BUDGET_GROUP_CODE or
T.DESCRSHORT <> S.DESCRSHORT or
T.OPRID <> S.OPRID or
T.TERM_TYPE <> S.TERM_TYPE or
T.ACAD_PROG_PRIMARY <> S.ACAD_PROG_PRIMARY or
T.ACAD_PLAN <> S.ACAD_PLAN or
T.ACAD_SUB_PLAN <> S.ACAD_SUB_PLAN or
T.ACADEMIC_LOAD <> S.ACADEMIC_LOAD or
T.ACAD_LOAD_APPR <> S.ACAD_LOAD_APPR or
T.ACAD_LEVEL_PROJ <> S.ACAD_LEVEL_PROJ or
T.ACAD_LEVEL_BOT <> S.ACAD_LEVEL_BOT or
T.FA_LOAD <> S.FA_LOAD or
T.FORM_OF_STUDY <> S.FORM_OF_STUDY or
T.RESIDENCY <> S.RESIDENCY or
T.FIN_AID_FED_RES <> S.FIN_AID_FED_RES or
T.FIN_AID_ST_RES <> S.FIN_AID_ST_RES or
T.FIN_AID_FED_EXCPT <> S.FIN_AID_FED_EXCPT or
T.FIN_AID_ST_EXCPT <> S.FIN_AID_ST_EXCPT or
T.STATE_RESIDENCE <> S.STATE_RESIDENCE or
T.APP_STATE_RESIDENC <> S.APP_STATE_RESIDENC or
T.HOUSING_TYPE <> S.HOUSING_TYPE or
T.FED_TERM_COA <> S.FED_TERM_COA or
T.INST_TERM_COA <> S.INST_TERM_COA or
T.PELL_TERM_COA <> S.PELL_TERM_COA or
T.SFA_PELTRM_COA_LHT <> S.SFA_PELTRM_COA_LHT or
T.MARITAL_STAT <> S.MARITAL_STAT or
T.NUMBER_IN_FAMILY <> S.NUMBER_IN_FAMILY or
T.DEPNDNCY_STAT <> S.DEPNDNCY_STAT or
T.NSLDS_LOAN_YEAR <> S.NSLDS_LOAN_YEAR or
T.PRORATE_BUDGET <> S.PRORATE_BUDGET or
T.POSTAL <> S.POSTAL or
T.FA_UNIT_ANTIC <> S.FA_UNIT_ANTIC or
T.FA_UNIT_COMPLETED <> S.FA_UNIT_COMPLETED or
T.FA_UNIT_IN_PROG <> S.FA_UNIT_IN_PROG or
T.FA_UNIT_CURRENT <> S.FA_UNIT_CURRENT or
T.ACAD_PLAN_TYPE <> S.ACAD_PLAN_TYPE or
nvl(trim(T.FA_TERM_EFFDT),0) <> nvl(trim(S.FA_TERM_EFFDT),0) or
T.FA_TERM_EFFSEQ <> S.FA_TERM_EFFSEQ or
T.FA_LOAD_CURRENT <> S.FA_LOAD_CURRENT or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ACAD_CAREER,
T.STRM,
T.EFFDT,
T.EFFSEQ,
T.SRC_SYS_ID,
T.BUDGET_GROUP_CODE,
T.DESCRSHORT,
T.OPRID,
T.TERM_TYPE,
T.ACAD_PROG_PRIMARY,
T.ACAD_PLAN,
T.ACAD_SUB_PLAN,
T.ACADEMIC_LOAD,
T.ACAD_LOAD_APPR,
T.ACAD_LEVEL_PROJ,
T.ACAD_LEVEL_BOT,
T.FA_LOAD,
T.FORM_OF_STUDY,
T.RESIDENCY,
T.FIN_AID_FED_RES,
T.FIN_AID_ST_RES,
T.FIN_AID_FED_EXCPT,
T.FIN_AID_ST_EXCPT,
T.STATE_RESIDENCE,
T.APP_STATE_RESIDENC,
T.HOUSING_TYPE,
T.FED_TERM_COA,
T.INST_TERM_COA,
T.PELL_TERM_COA,
T.SFA_PELTRM_COA_LHT,
T.MARITAL_STAT,
T.NUMBER_IN_FAMILY,
T.DEPNDNCY_STAT,
T.NSLDS_LOAN_YEAR,
T.PRORATE_BUDGET,
T.POSTAL,
T.FA_UNIT_ANTIC,
T.FA_UNIT_COMPLETED,
T.FA_UNIT_IN_PROG,
T.FA_UNIT_CURRENT,
T.ACAD_PLAN_TYPE,
T.FA_TERM_EFFDT,
T.FA_TERM_EFFSEQ,
T.FA_LOAD_CURRENT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.AID_YEAR,
S.ACAD_CAREER,
S.STRM,
S.EFFDT,
S.EFFSEQ,
'CS90',
S.BUDGET_GROUP_CODE,
S.DESCRSHORT,
S.OPRID,
S.TERM_TYPE,
S.ACAD_PROG_PRIMARY,
S.ACAD_PLAN,
S.ACAD_SUB_PLAN,
S.ACADEMIC_LOAD,
S.ACAD_LOAD_APPR,
S.ACAD_LEVEL_PROJ,
S.ACAD_LEVEL_BOT,
S.FA_LOAD,
S.FORM_OF_STUDY,
S.RESIDENCY,
S.FIN_AID_FED_RES,
S.FIN_AID_ST_RES,
S.FIN_AID_FED_EXCPT,
S.FIN_AID_ST_EXCPT,
S.STATE_RESIDENCE,
S.APP_STATE_RESIDENC,
S.HOUSING_TYPE,
S.FED_TERM_COA,
S.INST_TERM_COA,
S.PELL_TERM_COA,
S.SFA_PELTRM_COA_LHT,
S.MARITAL_STAT,
S.NUMBER_IN_FAMILY,
S.DEPNDNCY_STAT,
S.NSLDS_LOAN_YEAR,
S.PRORATE_BUDGET,
S.POSTAL,
S.FA_UNIT_ANTIC,
S.FA_UNIT_COMPLETED,
S.FA_UNIT_IN_PROG,
S.FA_UNIT_CURRENT,
S.ACAD_PLAN_TYPE,
S.FA_TERM_EFFDT,
S.FA_TERM_EFFSEQ,
S.FA_LOAD_CURRENT,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_TERM_BDGT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_TERM_BDGT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_TERM_BDGT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_TERM_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_TERM_BDGT';
update CSSTG_OWNER.PS_STDNT_TERM_BDGT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ
   from CSSTG_OWNER.PS_STDNT_TERM_BDGT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TERM_BDGT') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ
   from SYSADM.PS_STDNT_TERM_BDGT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_TERM_BDGT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STRM = S.STRM
   and T.EFFDT = S.EFFDT
   and T.EFFSEQ = S.EFFSEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_TERM_BDGT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_TERM_BDGT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_TERM_BDGT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END PS_STDNT_TERM_BDGT_P;
/

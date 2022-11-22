DROP PROCEDURE CSMRT_OWNER.AM_PS_STDNT_ENRL_P
/

--
-- AM_PS_STDNT_ENRL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_STDNT_ENRL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_ENRL from PeopleSoft table PS_STDNT_ENRL.
--54/10/2017,    Jim Doucette 
--                              Converted from PS_STDNT_ENRL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_STDNT_ENRL';
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
 where TABLE_NAME = 'PS_STDNT_ENRL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_ENRL@AMSOURCE S)
 where TABLE_NAME = 'PS_STDNT_ENRL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_STDNT_ENRL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_STDNT_ENRL T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STRM),'-') STRM,
nvl(CLASS_NBR,0) CLASS_NBR,
nvl(trim(CRSE_CAREER),'-') CRSE_CAREER,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(SESSN_ENRL_CNTL),'-') SESSN_ENRL_CNTL,
nvl(trim(STDNT_ENRL_STATUS),'-') STDNT_ENRL_STATUS,
nvl(trim(ENRL_STATUS_REASON),'-') ENRL_STATUS_REASON,
nvl(trim(ENRL_ACTION_LAST),'-') ENRL_ACTION_LAST,
nvl(trim(ENRL_ACTN_RSN_LAST),'-') ENRL_ACTN_RSN_LAST,
nvl(trim(ENRL_ACTN_PRC_LAST),'-') ENRL_ACTN_PRC_LAST,
to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT,
to_date(to_char(case when ENRL_ADD_DT < '01-JAN-1800' then NULL else ENRL_ADD_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ENRL_ADD_DT,
to_date(to_char(case when ENRL_DROP_DT < '01-JAN-1800' then NULL else ENRL_DROP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ENRL_DROP_DT,
nvl(UNT_TAKEN,0) UNT_TAKEN,
nvl(UNT_PRGRSS,0) UNT_PRGRSS,
nvl(UNT_PRGRSS_FA,0) UNT_PRGRSS_FA,
nvl(UNT_BILLING,0) UNT_BILLING,
nvl(CRSE_COUNT,0) CRSE_COUNT,
nvl(trim(GRADING_BASIS_ENRL),'-') GRADING_BASIS_ENRL,
to_date(to_char(case when GRADING_BASIS_DT < '01-JAN-1800' then NULL else GRADING_BASIS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') GRADING_BASIS_DT,
nvl(trim(OVRD_GRADING_BASIS),'-') OVRD_GRADING_BASIS,
nvl(trim(CRSE_GRADE_OFF),'-') CRSE_GRADE_OFF,
nvl(trim(CRSE_GRADE_INPUT),'-') CRSE_GRADE_INPUT,
to_date(to_char(case when GRADE_DT < '01-JAN-1800' then NULL else GRADE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') GRADE_DT,
nvl(trim(REPEAT_CODE),'-') REPEAT_CODE,
to_date(to_char(case when REPEAT_DT < '01-JAN-1800' then NULL else REPEAT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') REPEAT_DT,
nvl(CLASS_PRMSN_NBR,0) CLASS_PRMSN_NBR,
nvl(ASSOCIATED_CLASS,0) ASSOCIATED_CLASS,
nvl(STDNT_POSITIN,0) STDNT_POSITIN,
nvl(trim(AUDIT_GRADE_BASIS),'-') AUDIT_GRADE_BASIS,
nvl(trim(EARN_CREDIT),'-') EARN_CREDIT,
nvl(trim(INCLUDE_IN_GPA),'-') INCLUDE_IN_GPA,
nvl(trim(UNITS_ATTEMPTED),'-') UNITS_ATTEMPTED,
nvl(GRADE_POINTS,0) GRADE_POINTS,
nvl(GRADE_POINTS_FA,0) GRADE_POINTS_FA,
nvl(GRD_PTS_PER_UNIT,0) GRD_PTS_PER_UNIT,
nvl(trim(MANDATORY_GRD_BAS),'-') MANDATORY_GRD_BAS,
nvl(RSRV_CAP_NBR,0) RSRV_CAP_NBR,
nvl(trim(RQMNT_DESIGNTN),'-') RQMNT_DESIGNTN,
nvl(trim(RQMNT_DESIGNTN_OPT),'-') RQMNT_DESIGNTN_OPT,
nvl(trim(RQMNT_DESIGNTN_GRD),'-') RQMNT_DESIGNTN_GRD,
nvl(trim(INSTRUCTOR_ID),'-') INSTRUCTOR_ID,
nvl(DROP_CLASS_IF_ENRL,0) DROP_CLASS_IF_ENRL,
nvl(trim(ASSOCIATION_99),'-') ASSOCIATION_99,
nvl(trim(OPRID),'-') OPRID,
nvl(trim(TSCRPT_NOTE_ID),'-') TSCRPT_NOTE_ID,
nvl(trim(TSCRPT_NOTE_EXISTS),'-') TSCRPT_NOTE_EXISTS,
nvl(trim(NOTIFY_STDNT_CHNG),'-') NOTIFY_STDNT_CHNG,
nvl(trim(REPEAT_CANDIDATE),'-') REPEAT_CANDIDATE,
nvl(trim(VALID_ATTEMPT),'-') VALID_ATTEMPT,
nvl(trim(GRADE_CATEGORY),'-') GRADE_CATEGORY,
nvl(trim(SEL_GROUP),'-') SEL_GROUP,
nvl(DYN_CLASS_NBR,0) DYN_CLASS_NBR,
nvl(UNT_EARNED,0) UNT_EARNED,
to_date(to_char(case when LAST_UPD_DT_STMP < '01-JAN-1800' then NULL else LAST_UPD_DT_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_UPD_DT_STMP,
to_date(to_char(case when LAST_UPD_TM_STMP < '01-JAN-1800' then NULL else LAST_UPD_TM_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_UPD_TM_STMP,
to_date(to_char(case when LAST_ENRL_DT_STMP < '01-JAN-1800' then NULL else LAST_ENRL_DT_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_ENRL_DT_STMP,
to_date(to_char(case when LAST_ENRL_TM_STMP < '01-JAN-1800' then NULL else LAST_ENRL_TM_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_ENRL_TM_STMP,
to_date(to_char(case when LAST_DROP_DT_STMP < '01-JAN-1800' then NULL else LAST_DROP_DT_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_DROP_DT_STMP,
to_date(to_char(case when LAST_DROP_TM_STMP < '01-JAN-1800' then NULL else LAST_DROP_TM_STMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_DROP_TM_STMP,
nvl(trim(ENRL_REQ_SOURCE),'-') ENRL_REQ_SOURCE,
nvl(trim(LAST_UPD_ENREQ_SRC),'-') LAST_UPD_ENREQ_SRC,
nvl(trim(GRADING_SCHEME_ENR),'-') GRADING_SCHEME_ENR,
nvl(RELATE_CLASS_NBR_1,0) RELATE_CLASS_NBR_1,
nvl(RELATE_CLASS_NBR_2,0) RELATE_CLASS_NBR_2,
nvl(trim(ACAD_PROG),'-') ACAD_PROG
from SYSADM.PS_STDNT_ENRL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL') 
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8) S
   on (
T.EMPLID = S.EMPLID and
T.ACAD_CAREER = S.ACAD_CAREER and
T.INSTITUTION = S.INSTITUTION and
T.STRM = S.STRM and
T.CLASS_NBR = S.CLASS_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.CRSE_CAREER = S.CRSE_CAREER,
T.SESSION_CODE = S.SESSION_CODE,
T.SESSN_ENRL_CNTL = S.SESSN_ENRL_CNTL,
T.STDNT_ENRL_STATUS = S.STDNT_ENRL_STATUS,
T.ENRL_STATUS_REASON = S.ENRL_STATUS_REASON,
T.ENRL_ACTION_LAST = S.ENRL_ACTION_LAST,
T.ENRL_ACTN_RSN_LAST = S.ENRL_ACTN_RSN_LAST,
T.ENRL_ACTN_PRC_LAST = S.ENRL_ACTN_PRC_LAST,
T.STATUS_DT = S.STATUS_DT,
T.ENRL_ADD_DT = S.ENRL_ADD_DT,
T.ENRL_DROP_DT = S.ENRL_DROP_DT,
T.UNT_TAKEN = S.UNT_TAKEN,
T.UNT_PRGRSS = S.UNT_PRGRSS,
T.UNT_PRGRSS_FA = S.UNT_PRGRSS_FA,
T.UNT_BILLING = S.UNT_BILLING,
T.CRSE_COUNT = S.CRSE_COUNT,
T.GRADING_BASIS_ENRL = S.GRADING_BASIS_ENRL,
T.GRADING_BASIS_DT = S.GRADING_BASIS_DT,
T.OVRD_GRADING_BASIS = S.OVRD_GRADING_BASIS,
T.CRSE_GRADE_OFF = S.CRSE_GRADE_OFF,
T.CRSE_GRADE_INPUT = S.CRSE_GRADE_INPUT,
T.GRADE_DT = S.GRADE_DT,
T.REPEAT_CODE = S.REPEAT_CODE,
T.REPEAT_DT = S.REPEAT_DT,
T.CLASS_PRMSN_NBR = S.CLASS_PRMSN_NBR,
T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS,
T.STDNT_POSITIN = S.STDNT_POSITIN,
T.AUDIT_GRADE_BASIS = S.AUDIT_GRADE_BASIS,
T.EARN_CREDIT = S.EARN_CREDIT,
T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
T.UNITS_ATTEMPTED = S.UNITS_ATTEMPTED,
T.GRADE_POINTS = S.GRADE_POINTS,
T.GRADE_POINTS_FA = S.GRADE_POINTS_FA,
T.GRD_PTS_PER_UNIT = S.GRD_PTS_PER_UNIT,
T.MANDATORY_GRD_BAS = S.MANDATORY_GRD_BAS,
T.RSRV_CAP_NBR = S.RSRV_CAP_NBR,
T.RQMNT_DESIGNTN = S.RQMNT_DESIGNTN,
T.RQMNT_DESIGNTN_OPT = S.RQMNT_DESIGNTN_OPT,
T.RQMNT_DESIGNTN_GRD = S.RQMNT_DESIGNTN_GRD,
T.INSTRUCTOR_ID = S.INSTRUCTOR_ID,
T.DROP_CLASS_IF_ENRL = S.DROP_CLASS_IF_ENRL,
T.ASSOCIATION_99 = S.ASSOCIATION_99,
T.OPRID = S.OPRID,
T.TSCRPT_NOTE_ID = S.TSCRPT_NOTE_ID,
T.TSCRPT_NOTE_EXISTS = S.TSCRPT_NOTE_EXISTS,
T.NOTIFY_STDNT_CHNG = S.NOTIFY_STDNT_CHNG,
T.REPEAT_CANDIDATE = S.REPEAT_CANDIDATE,
T.VALID_ATTEMPT = S.VALID_ATTEMPT,
T.GRADE_CATEGORY = S.GRADE_CATEGORY,
T.SEL_GROUP = S.SEL_GROUP,
T.DYN_CLASS_NBR = S.DYN_CLASS_NBR,
T.UNT_EARNED = S.UNT_EARNED,
T.LAST_UPD_DT_STMP = S.LAST_UPD_DT_STMP,
T.LAST_UPD_TM_STMP = S.LAST_UPD_TM_STMP,
T.LAST_ENRL_DT_STMP = S.LAST_ENRL_DT_STMP,
T.LAST_ENRL_TM_STMP = S.LAST_ENRL_TM_STMP,
T.LAST_DROP_DT_STMP = S.LAST_DROP_DT_STMP,
T.LAST_DROP_TM_STMP = S.LAST_DROP_TM_STMP,
T.ENRL_REQ_SOURCE = S.ENRL_REQ_SOURCE,
T.LAST_UPD_ENREQ_SRC = S.LAST_UPD_ENREQ_SRC,
T.GRADING_SCHEME_ENR = S.GRADING_SCHEME_ENR,
T.RELATE_CLASS_NBR_1 = S.RELATE_CLASS_NBR_1,
T.RELATE_CLASS_NBR_2 = S.RELATE_CLASS_NBR_2,
T.ACAD_PROG = S.ACAD_PROG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.CRSE_CAREER <> S.CRSE_CAREER or
T.SESSION_CODE <> S.SESSION_CODE or
T.SESSN_ENRL_CNTL <> S.SESSN_ENRL_CNTL or
T.STDNT_ENRL_STATUS <> S.STDNT_ENRL_STATUS or
T.ENRL_STATUS_REASON <> S.ENRL_STATUS_REASON or
T.ENRL_ACTION_LAST <> S.ENRL_ACTION_LAST or
T.ENRL_ACTN_RSN_LAST <> S.ENRL_ACTN_RSN_LAST or
T.ENRL_ACTN_PRC_LAST <> S.ENRL_ACTN_PRC_LAST or
nvl(trim(T.STATUS_DT),0) <> nvl(trim(S.STATUS_DT),0) or
nvl(trim(T.ENRL_ADD_DT),0) <> nvl(trim(S.ENRL_ADD_DT),0) or
nvl(trim(T.ENRL_DROP_DT),0) <> nvl(trim(S.ENRL_DROP_DT),0) or
T.UNT_TAKEN <> S.UNT_TAKEN or
T.UNT_PRGRSS <> S.UNT_PRGRSS or
T.UNT_PRGRSS_FA <> S.UNT_PRGRSS_FA or
T.UNT_BILLING <> S.UNT_BILLING or
T.CRSE_COUNT <> S.CRSE_COUNT or
T.GRADING_BASIS_ENRL <> S.GRADING_BASIS_ENRL or
nvl(trim(T.GRADING_BASIS_DT),0) <> nvl(trim(S.GRADING_BASIS_DT),0) or
T.OVRD_GRADING_BASIS <> S.OVRD_GRADING_BASIS or
T.CRSE_GRADE_OFF <> S.CRSE_GRADE_OFF or
T.CRSE_GRADE_INPUT <> S.CRSE_GRADE_INPUT or
nvl(trim(T.GRADE_DT),0) <> nvl(trim(S.GRADE_DT),0) or
T.REPEAT_CODE <> S.REPEAT_CODE or
nvl(trim(T.REPEAT_DT),0) <> nvl(trim(S.REPEAT_DT),0) or
T.CLASS_PRMSN_NBR <> S.CLASS_PRMSN_NBR or
T.ASSOCIATED_CLASS <> S.ASSOCIATED_CLASS or
T.STDNT_POSITIN <> S.STDNT_POSITIN or
T.AUDIT_GRADE_BASIS <> S.AUDIT_GRADE_BASIS or
T.EARN_CREDIT <> S.EARN_CREDIT or
T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA or
T.UNITS_ATTEMPTED <> S.UNITS_ATTEMPTED or
T.GRADE_POINTS <> S.GRADE_POINTS or
T.GRADE_POINTS_FA <> S.GRADE_POINTS_FA or
T.GRD_PTS_PER_UNIT <> S.GRD_PTS_PER_UNIT or
T.MANDATORY_GRD_BAS <> S.MANDATORY_GRD_BAS or
T.RSRV_CAP_NBR <> S.RSRV_CAP_NBR or
T.RQMNT_DESIGNTN <> S.RQMNT_DESIGNTN or
T.RQMNT_DESIGNTN_OPT <> S.RQMNT_DESIGNTN_OPT or
T.RQMNT_DESIGNTN_GRD <> S.RQMNT_DESIGNTN_GRD or
T.INSTRUCTOR_ID <> S.INSTRUCTOR_ID or
T.DROP_CLASS_IF_ENRL <> S.DROP_CLASS_IF_ENRL or
T.ASSOCIATION_99 <> S.ASSOCIATION_99 or
T.OPRID <> S.OPRID or
T.TSCRPT_NOTE_ID <> S.TSCRPT_NOTE_ID or
T.TSCRPT_NOTE_EXISTS <> S.TSCRPT_NOTE_EXISTS or
T.NOTIFY_STDNT_CHNG <> S.NOTIFY_STDNT_CHNG or
T.REPEAT_CANDIDATE <> S.REPEAT_CANDIDATE or
T.VALID_ATTEMPT <> S.VALID_ATTEMPT or
T.GRADE_CATEGORY <> S.GRADE_CATEGORY or
T.SEL_GROUP <> S.SEL_GROUP or
T.DYN_CLASS_NBR <> S.DYN_CLASS_NBR or
T.UNT_EARNED <> S.UNT_EARNED or
nvl(trim(T.LAST_UPD_DT_STMP),0) <> nvl(trim(S.LAST_UPD_DT_STMP),0) or
nvl(trim(T.LAST_UPD_TM_STMP),0) <> nvl(trim(S.LAST_UPD_TM_STMP),0) or
nvl(trim(T.LAST_ENRL_DT_STMP),0) <> nvl(trim(S.LAST_ENRL_DT_STMP),0) or
nvl(trim(T.LAST_ENRL_TM_STMP),0) <> nvl(trim(S.LAST_ENRL_TM_STMP),0) or
nvl(trim(T.LAST_DROP_DT_STMP),0) <> nvl(trim(S.LAST_DROP_DT_STMP),0) or
nvl(trim(T.LAST_DROP_TM_STMP),0) <> nvl(trim(S.LAST_DROP_TM_STMP),0) or
T.ENRL_REQ_SOURCE <> S.ENRL_REQ_SOURCE or
T.LAST_UPD_ENREQ_SRC <> S.LAST_UPD_ENREQ_SRC or
T.GRADING_SCHEME_ENR <> S.GRADING_SCHEME_ENR or
T.RELATE_CLASS_NBR_1 <> S.RELATE_CLASS_NBR_1 or
T.RELATE_CLASS_NBR_2 <> S.RELATE_CLASS_NBR_2 or
T.ACAD_PROG <> S.ACAD_PROG or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.ACAD_CAREER,
T.INSTITUTION,
T.STRM,
T.CLASS_NBR,
T.SRC_SYS_ID,
T.CRSE_CAREER,
T.SESSION_CODE,
T.SESSN_ENRL_CNTL,
T.STDNT_ENRL_STATUS,
T.ENRL_STATUS_REASON,
T.ENRL_ACTION_LAST,
T.ENRL_ACTN_RSN_LAST,
T.ENRL_ACTN_PRC_LAST,
T.STATUS_DT,
T.ENRL_ADD_DT,
T.ENRL_DROP_DT,
T.UNT_TAKEN,
T.UNT_PRGRSS,
T.UNT_PRGRSS_FA,
T.UNT_BILLING,
T.CRSE_COUNT,
T.GRADING_BASIS_ENRL,
T.GRADING_BASIS_DT,
T.OVRD_GRADING_BASIS,
T.CRSE_GRADE_OFF,
T.CRSE_GRADE_INPUT,
T.GRADE_DT,
T.REPEAT_CODE,
T.REPEAT_DT,
T.CLASS_PRMSN_NBR,
T.ASSOCIATED_CLASS,
T.STDNT_POSITIN,
T.AUDIT_GRADE_BASIS,
T.EARN_CREDIT,
T.INCLUDE_IN_GPA,
T.UNITS_ATTEMPTED,
T.GRADE_POINTS,
T.GRADE_POINTS_FA,
T.GRD_PTS_PER_UNIT,
T.MANDATORY_GRD_BAS,
T.RSRV_CAP_NBR,
T.RQMNT_DESIGNTN,
T.RQMNT_DESIGNTN_OPT,
T.RQMNT_DESIGNTN_GRD,
T.INSTRUCTOR_ID,
T.DROP_CLASS_IF_ENRL,
T.ASSOCIATION_99,
T.OPRID,
T.TSCRPT_NOTE_ID,
T.TSCRPT_NOTE_EXISTS,
T.NOTIFY_STDNT_CHNG,
T.REPEAT_CANDIDATE,
T.VALID_ATTEMPT,
T.GRADE_CATEGORY,
T.SEL_GROUP,
T.DYN_CLASS_NBR,
T.UNT_EARNED,
T.LAST_UPD_DT_STMP,
T.LAST_UPD_TM_STMP,
T.LAST_ENRL_DT_STMP,
T.LAST_ENRL_TM_STMP,
T.LAST_DROP_DT_STMP,
T.LAST_DROP_TM_STMP,
T.ENRL_REQ_SOURCE,
T.LAST_UPD_ENREQ_SRC,
T.GRADING_SCHEME_ENR,
T.RELATE_CLASS_NBR_1,
T.RELATE_CLASS_NBR_2,
T.ACAD_PROG,
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
S.STRM,
S.CLASS_NBR,
'CS90',
S.CRSE_CAREER,
S.SESSION_CODE,
S.SESSN_ENRL_CNTL,
S.STDNT_ENRL_STATUS,
S.ENRL_STATUS_REASON,
S.ENRL_ACTION_LAST,
S.ENRL_ACTN_RSN_LAST,
S.ENRL_ACTN_PRC_LAST,
S.STATUS_DT,
S.ENRL_ADD_DT,
S.ENRL_DROP_DT,
S.UNT_TAKEN,
S.UNT_PRGRSS,
S.UNT_PRGRSS_FA,
S.UNT_BILLING,
S.CRSE_COUNT,
S.GRADING_BASIS_ENRL,
S.GRADING_BASIS_DT,
S.OVRD_GRADING_BASIS,
S.CRSE_GRADE_OFF,
S.CRSE_GRADE_INPUT,
S.GRADE_DT,
S.REPEAT_CODE,
S.REPEAT_DT,
S.CLASS_PRMSN_NBR,
S.ASSOCIATED_CLASS,
S.STDNT_POSITIN,
S.AUDIT_GRADE_BASIS,
S.EARN_CREDIT,
S.INCLUDE_IN_GPA,
S.UNITS_ATTEMPTED,
S.GRADE_POINTS,
S.GRADE_POINTS_FA,
S.GRD_PTS_PER_UNIT,
S.MANDATORY_GRD_BAS,
S.RSRV_CAP_NBR,
S.RQMNT_DESIGNTN,
S.RQMNT_DESIGNTN_OPT,
S.RQMNT_DESIGNTN_GRD,
S.INSTRUCTOR_ID,
S.DROP_CLASS_IF_ENRL,
S.ASSOCIATION_99,
S.OPRID,
S.TSCRPT_NOTE_ID,
S.TSCRPT_NOTE_EXISTS,
S.NOTIFY_STDNT_CHNG,
S.REPEAT_CANDIDATE,
S.VALID_ATTEMPT,
S.GRADE_CATEGORY,
S.SEL_GROUP,
S.DYN_CLASS_NBR,
S.UNT_EARNED,
S.LAST_UPD_DT_STMP,
S.LAST_UPD_TM_STMP,
S.LAST_ENRL_DT_STMP,
S.LAST_ENRL_TM_STMP,
S.LAST_DROP_DT_STMP,
S.LAST_DROP_TM_STMP,
S.ENRL_REQ_SOURCE,
S.LAST_UPD_ENREQ_SRC,
S.GRADING_SCHEME_ENR,
S.RELATE_CLASS_NBR_1,
S.RELATE_CLASS_NBR_2,
S.ACAD_PROG,
'N',
'S',
sysdate,
sysdate,
1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_ENRL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_ENRL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_ENRL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_ENRL';
update AMSTG_OWNER.PS_STDNT_ENRL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR
   from AMSTG_OWNER.PS_STDNT_ENRL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, STRM, CLASS_NBR
   from SYSADM.PS_STDNT_ENRL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_ENRL') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.STRM = S.STRM
   and T.CLASS_NBR = S.CLASS_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_ENRL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_ENRL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_ENRL'
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

END AM_PS_STDNT_ENRL_P;
/

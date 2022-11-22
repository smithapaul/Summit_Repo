DROP PROCEDURE CSMRT_OWNER.PS_CLASS_TBL_P
/

--
-- PS_CLASS_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CLASS_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLASS_TBL from PeopleSoft table PS_CLASS_TBL.
--
-- V01  SMT-xxxx 04/21/2017,    Jim Doucette
--                              Converted from PS_CLASS_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CLASS_TBL';
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
 where TABLE_NAME = 'PS_CLASS_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_CLASS_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CLASS_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CLASS_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLASS_TBL T
using (select /*+ full(S) */
    nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    nvl(trim(CLASS_SECTION),'-') CLASS_SECTION, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_GROUP),'-') ACAD_GROUP, 
    nvl(trim(SUBJECT),'-') SUBJECT, 
    nvl(trim(CATALOG_NBR),'-') CATALOG_NBR, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    REPLACE(nvl(trim(DESCR),'-'), '  ', ' ') DESCR,
    nvl(CLASS_NBR,0) CLASS_NBR, 
    nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT, 
    nvl(trim(ENRL_STAT),'-') ENRL_STAT, 
    nvl(trim(CLASS_STAT),'-') CLASS_STAT, 
    nvl(trim(CLASS_TYPE),'-') CLASS_TYPE, 
    nvl(ASSOCIATED_CLASS,0) ASSOCIATED_CLASS, 
    nvl(trim(WAITLIST_DAEMON),'-') WAITLIST_DAEMON, 
    nvl(trim(AUTO_ENRL_WAITLIST),'-') AUTO_ENRL_WAITLIST, 
    nvl(trim(STDNT_SPEC_PERM),'-') STDNT_SPEC_PERM, 
    nvl(trim(AUTO_ENROLL_SECT_1),'-') AUTO_ENROLL_SECT_1, 
    nvl(trim(AUTO_ENROLL_SECT_2),'-') AUTO_ENROLL_SECT_2, 
    nvl(trim(RESECTION),'-') RESECTION, 
    nvl(trim(SCHEDULE_PRINT),'-') SCHEDULE_PRINT, 
    nvl(trim(CONSENT),'-') CONSENT, 
    nvl(ENRL_CAP,0) ENRL_CAP, 
    nvl(WAIT_CAP,0) WAIT_CAP, 
    nvl(MIN_ENRL,0) MIN_ENRL, 
    nvl(ENRL_TOT,0) ENRL_TOT, 
    nvl(WAIT_TOT,0) WAIT_TOT, 
    nvl(CRS_TOPIC_ID,0) CRS_TOPIC_ID, 
    nvl(trim(PRINT_TOPIC),'-') PRINT_TOPIC, 
    nvl(trim(ACAD_ORG),'-') ACAD_ORG, 
    nvl(NEXT_STDNT_POSITIN,0) NEXT_STDNT_POSITIN, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(CAMPUS),'-') CAMPUS, 
    nvl(trim(LOCATION),'-') LOCATION, 
    nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR, 
    nvl(trim(INSTRUCTION_MODE),'-') INSTRUCTION_MODE, 
    nvl(trim(EQUIV_CRSE_ID),'-') EQUIV_CRSE_ID, 
    nvl(trim(OVRD_CRSE_EQUIV_ID),'-') OVRD_CRSE_EQUIV_ID, 
    nvl(ROOM_CAP_REQUEST,0) ROOM_CAP_REQUEST, 
    NVL(START_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) START_DT,
    NVL(END_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) END_DT,
    CANCEL_DT,
    nvl(trim(PRIM_INSTR_SECT),'-') PRIM_INSTR_SECT, 
    nvl(trim(COMBINED_SECTION),'-') COMBINED_SECTION, 
    nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
    nvl(EXAM_SEAT_SPACING,0) EXAM_SEAT_SPACING, 
    nvl(trim(DYN_DT_INCLUDE),'-') DYN_DT_INCLUDE, 
    nvl(trim(DYN_DT_CALC_REQ),'-') DYN_DT_CALC_REQ, 
    nvl(trim(ATTEND_GENERATE),'-') ATTEND_GENERATE, 
    nvl(trim(ATTEND_SYNC_REQD),'-') ATTEND_SYNC_REQD, 
    nvl(trim(FEES_EXIST),'-') FEES_EXIST, 
    nvl(trim(CNCL_IF_STUD_ENRLD),'-') CNCL_IF_STUD_ENRLD, 
    nvl(trim(RCV_FROM_ITEM_TYPE),'-') RCV_FROM_ITEM_TYPE, 
    nvl(trim(AP_BUS_UNIT),'-') AP_BUS_UNIT, 
    nvl(trim(AP_LEDGER),'-') AP_LEDGER, 
    nvl(trim(AP_ACCOUNT),'-') AP_ACCOUNT, 
    nvl(trim(AP_DEPTID),'-') AP_DEPTID, 
    nvl(trim(AP_PROJ_ID),'-') AP_PROJ_ID, 
    nvl(trim(AP_PRODUCT),'-') AP_PRODUCT, 
    nvl(trim(AP_FUND_CODE),'-') AP_FUND_CODE, 
    nvl(trim(AP_PROG_CODE),'-') AP_PROG_CODE, 
    nvl(trim(AP_CLASS_FLD),'-') AP_CLASS_FLD, 
    nvl(trim(AP_AFFILIATE),'-') AP_AFFILIATE, 
    nvl(trim(AP_OP_UNIT),'-') AP_OP_UNIT, 
    nvl(trim(AP_ALTACCT),'-') AP_ALTACCT, 
    nvl(trim(AP_BUD_REF),'-') AP_BUD_REF, 
    nvl(trim(AP_CF1),'-') AP_CF1, 
    nvl(trim(AP_CF2),'-') AP_CF2, 
    nvl(trim(AP_CF3),'-') AP_CF3, 
    nvl(trim(AP_AFF_INT1),'-') AP_AFF_INT1, 
    nvl(trim(AP_AFF_INT2),'-') AP_AFF_INT2, 
    nvl(trim(WRITEOFF_BUS_UNIT),'-') WRITEOFF_BUS_UNIT, 
    nvl(trim(WRITEOFF_LEDGER),'-') WRITEOFF_LEDGER, 
    nvl(trim(WRITEOFF_ACCOUNT),'-') WRITEOFF_ACCOUNT, 
    nvl(trim(WRITEOFF_DEPTID),'-') WRITEOFF_DEPTID, 
    nvl(trim(WRITEOFF_PROJ_ID),'-') WRITEOFF_PROJ_ID, 
    nvl(trim(WRITEOFF_PRODUCT),'-') WRITEOFF_PRODUCT, 
    nvl(trim(WRITEOFF_FUND_CODE),'-') WRITEOFF_FUND_CODE, 
    nvl(trim(WRITEOFF_PROG_CODE),'-') WRITEOFF_PROG_CODE, 
    nvl(trim(WRITEOFF_CLASS_FLD),'-') WRITEOFF_CLASS_FLD, 
    nvl(trim(WRITEOFF_AFFILIATE),'-') WRITEOFF_AFFILIATE, 
    nvl(trim(WRITEOFF_OP_UNIT),'-') WRITEOFF_OP_UNIT, 
    nvl(trim(WRITEOFF_ALTACCT),'-') WRITEOFF_ALTACCT, 
    nvl(trim(WRITEOFF_BUD_REF),'-') WRITEOFF_BUD_REF, 
    nvl(trim(WRITEOFF_CF1),'-') WRITEOFF_CF1, 
    nvl(trim(WRITEOFF_CF2),'-') WRITEOFF_CF2, 
    nvl(trim(WRITEOFF_CF3),'-') WRITEOFF_CF3, 
    nvl(trim(WRITEOFF_AFF_INT1),'-') WRITEOFF_AFF_INT1, 
    nvl(trim(WRITEOFF_AFF_INT2),'-') WRITEOFF_AFF_INT2, 
    nvl(trim(EXT_WRITEOFF),'-') EXT_WRITEOFF, 
    nvl(trim(GL_INTERFACE_REQ),'-') GL_INTERFACE_REQ, 
    nvl(trim(LMS_FILE_TYPE),'-') LMS_FILE_TYPE, 
    nvl(trim(LMS_GROUP_ID),'-') LMS_GROUP_ID, 
    nvl(trim(LMS_URL),'-') LMS_URL, 
    LMS_CLASS_EXT_DTTM,
    LMS_ENRL_EXT_DTTM,
    nvl(trim(LMS_PROVIDER),'-') LMS_PROVIDER,
    nvl(trim(SSR_DROP_CONSENT),'-') SSR_DROP_CONSENT
from SYSADM.PS_CLASS_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_TBL') ) S
   on ( 
    T.CRSE_ID = S.CRSE_ID and 
    T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and 
    T.STRM = S.STRM and 
    T.SESSION_CODE = S.SESSION_CODE and 
    T.CLASS_SECTION = S.CLASS_SECTION and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.INSTITUTION = S.INSTITUTION,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.SUBJECT = S.SUBJECT,
    T.CATALOG_NBR = S.CATALOG_NBR,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.DESCR = S.DESCR,
    T.CLASS_NBR = S.CLASS_NBR,
    T.SSR_COMPONENT = S.SSR_COMPONENT,
    T.ENRL_STAT = S.ENRL_STAT,
    T.CLASS_STAT = S.CLASS_STAT,
    T.CLASS_TYPE = S.CLASS_TYPE,
    T.ASSOCIATED_CLASS = S.ASSOCIATED_CLASS,
    T.WAITLIST_DAEMON = S.WAITLIST_DAEMON,
    T.AUTO_ENRL_WAITLIST = S.AUTO_ENRL_WAITLIST,
    T.STDNT_SPEC_PERM = S.STDNT_SPEC_PERM,
    T.AUTO_ENROLL_SECT_1 = S.AUTO_ENROLL_SECT_1,
    T.AUTO_ENROLL_SECT_2 = S.AUTO_ENROLL_SECT_2,
    T.RESECTION = S.RESECTION,
    T.SCHEDULE_PRINT = S.SCHEDULE_PRINT,
    T.CONSENT = S.CONSENT,
    T.ENRL_CAP = S.ENRL_CAP,
    T.WAIT_CAP = S.WAIT_CAP,
    T.MIN_ENRL = S.MIN_ENRL,
    T.ENRL_TOT = S.ENRL_TOT,
    T.WAIT_TOT = S.WAIT_TOT,
    T.CRS_TOPIC_ID = S.CRS_TOPIC_ID,
    T.PRINT_TOPIC = S.PRINT_TOPIC,
    T.ACAD_ORG = S.ACAD_ORG,
    T.NEXT_STDNT_POSITIN = S.NEXT_STDNT_POSITIN,
    T.EMPLID = S.EMPLID,
    T.CAMPUS = S.CAMPUS,
    T.LOCATION = S.LOCATION,
    T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR,
    T.INSTRUCTION_MODE = S.INSTRUCTION_MODE,
    T.EQUIV_CRSE_ID = S.EQUIV_CRSE_ID,
    T.OVRD_CRSE_EQUIV_ID = S.OVRD_CRSE_EQUIV_ID,
    T.ROOM_CAP_REQUEST = S.ROOM_CAP_REQUEST,
    T.START_DT = S.START_DT,
    T.END_DT = S.END_DT,
    T.CANCEL_DT = S.CANCEL_DT,
    T.PRIM_INSTR_SECT = S.PRIM_INSTR_SECT,
    T.COMBINED_SECTION = S.COMBINED_SECTION,
    T.HOLIDAY_SCHEDULE = S.HOLIDAY_SCHEDULE,
    T.EXAM_SEAT_SPACING = S.EXAM_SEAT_SPACING,
    T.DYN_DT_INCLUDE = S.DYN_DT_INCLUDE,
    T.DYN_DT_CALC_REQ = S.DYN_DT_CALC_REQ,
    T.ATTEND_GENERATE = S.ATTEND_GENERATE,
    T.ATTEND_SYNC_REQD = S.ATTEND_SYNC_REQD,
    T.FEES_EXIST = S.FEES_EXIST,
    T.CNCL_IF_STUD_ENRLD = S.CNCL_IF_STUD_ENRLD,
    T.RCV_FROM_ITEM_TYPE = S.RCV_FROM_ITEM_TYPE,
    T.AP_BUS_UNIT = S.AP_BUS_UNIT,
    T.AP_LEDGER = S.AP_LEDGER,
    T.AP_ACCOUNT = S.AP_ACCOUNT,
    T.AP_DEPTID = S.AP_DEPTID,
    T.AP_PROJ_ID = S.AP_PROJ_ID,
    T.AP_PRODUCT = S.AP_PRODUCT,
    T.AP_FUND_CODE = S.AP_FUND_CODE,
    T.AP_PROG_CODE = S.AP_PROG_CODE,
    T.AP_CLASS_FLD = S.AP_CLASS_FLD,
    T.AP_AFFILIATE = S.AP_AFFILIATE,
    T.AP_OP_UNIT = S.AP_OP_UNIT,
    T.AP_ALTACCT = S.AP_ALTACCT,
    T.AP_BUD_REF = S.AP_BUD_REF,
    T.AP_CF1 = S.AP_CF1,
    T.AP_CF2 = S.AP_CF2,
    T.AP_CF3 = S.AP_CF3,
    T.AP_AFF_INT1 = S.AP_AFF_INT1,
    T.AP_AFF_INT2 = S.AP_AFF_INT2,
    T.WRITEOFF_BUS_UNIT = S.WRITEOFF_BUS_UNIT,
    T.WRITEOFF_LEDGER = S.WRITEOFF_LEDGER,
    T.WRITEOFF_ACCOUNT = S.WRITEOFF_ACCOUNT,
    T.WRITEOFF_DEPTID = S.WRITEOFF_DEPTID,
    T.WRITEOFF_PROJ_ID = S.WRITEOFF_PROJ_ID,
    T.WRITEOFF_PRODUCT = S.WRITEOFF_PRODUCT,
    T.WRITEOFF_FUND_CODE = S.WRITEOFF_FUND_CODE,
    T.WRITEOFF_PROG_CODE = S.WRITEOFF_PROG_CODE,
    T.WRITEOFF_CLASS_FLD = S.WRITEOFF_CLASS_FLD,
    T.WRITEOFF_AFFILIATE = S.WRITEOFF_AFFILIATE,
    T.WRITEOFF_OP_UNIT = S.WRITEOFF_OP_UNIT,
    T.WRITEOFF_ALTACCT = S.WRITEOFF_ALTACCT,
    T.WRITEOFF_BUD_REF = S.WRITEOFF_BUD_REF,
    T.WRITEOFF_CF1 = S.WRITEOFF_CF1,
    T.WRITEOFF_CF2 = S.WRITEOFF_CF2,
    T.WRITEOFF_CF3 = S.WRITEOFF_CF3,
    T.WRITEOFF_AFF_INT1 = S.WRITEOFF_AFF_INT1,
    T.WRITEOFF_AFF_INT2 = S.WRITEOFF_AFF_INT2,
    T.EXT_WRITEOFF = S.EXT_WRITEOFF,
    T.GL_INTERFACE_REQ = S.GL_INTERFACE_REQ,
    T.LMS_FILE_TYPE = S.LMS_FILE_TYPE,
    T.LMS_GROUP_ID = S.LMS_GROUP_ID,
    T.LMS_URL = S.LMS_URL,
    T.LMS_CLASS_EXT_DTTM = S.LMS_CLASS_EXT_DTTM,
    T.LMS_ENRL_EXT_DTTM = S.LMS_ENRL_EXT_DTTM,
    T.LMS_PROVIDER = S.LMS_PROVIDER,
    T.SSR_DROP_CONSENT = S.SSR_DROP_CONSENT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.ACAD_GROUP <> S.ACAD_GROUP or 
    T.SUBJECT <> S.SUBJECT or 
    T.CATALOG_NBR <> S.CATALOG_NBR or 
    T.ACAD_CAREER <> S.ACAD_CAREER or 
    T.DESCR <> S.DESCR or 
    T.CLASS_NBR <> S.CLASS_NBR or 
    T.SSR_COMPONENT <> S.SSR_COMPONENT or 
    T.ENRL_STAT <> S.ENRL_STAT or 
    T.CLASS_STAT <> S.CLASS_STAT or 
    T.CLASS_TYPE <> S.CLASS_TYPE or 
    T.ASSOCIATED_CLASS <> S.ASSOCIATED_CLASS or 
    T.WAITLIST_DAEMON <> S.WAITLIST_DAEMON or 
    T.AUTO_ENRL_WAITLIST <> S.AUTO_ENRL_WAITLIST or 
    T.STDNT_SPEC_PERM <> S.STDNT_SPEC_PERM or 
    T.AUTO_ENROLL_SECT_1 <> S.AUTO_ENROLL_SECT_1 or 
    T.AUTO_ENROLL_SECT_2 <> S.AUTO_ENROLL_SECT_2 or 
    T.RESECTION <> S.RESECTION or 
    T.SCHEDULE_PRINT <> S.SCHEDULE_PRINT or 
    T.CONSENT <> S.CONSENT or 
    T.ENRL_CAP <> S.ENRL_CAP or 
    T.WAIT_CAP <> S.WAIT_CAP or 
    T.MIN_ENRL <> S.MIN_ENRL or 
    T.ENRL_TOT <> S.ENRL_TOT or 
    T.WAIT_TOT <> S.WAIT_TOT or 
    T.CRS_TOPIC_ID <> S.CRS_TOPIC_ID or 
    T.PRINT_TOPIC <> S.PRINT_TOPIC or 
    T.ACAD_ORG <> S.ACAD_ORG or 
    T.NEXT_STDNT_POSITIN <> S.NEXT_STDNT_POSITIN or 
    T.EMPLID <> S.EMPLID or 
    T.CAMPUS <> S.CAMPUS or 
    T.LOCATION <> S.LOCATION or 
    T.CAMPUS_EVENT_NBR <> S.CAMPUS_EVENT_NBR or 
    T.INSTRUCTION_MODE <> S.INSTRUCTION_MODE or 
    T.EQUIV_CRSE_ID <> S.EQUIV_CRSE_ID or 
    T.OVRD_CRSE_EQUIV_ID <> S.OVRD_CRSE_EQUIV_ID or 
    T.ROOM_CAP_REQUEST <> S.ROOM_CAP_REQUEST or 
    T.START_DT <> S.START_DT or 
    T.END_DT <> S.END_DT or 
    nvl(trim(T.CANCEL_DT),0) <> nvl(trim(S.CANCEL_DT),0) or 
    T.PRIM_INSTR_SECT <> S.PRIM_INSTR_SECT or 
    T.COMBINED_SECTION <> S.COMBINED_SECTION or 
    T.HOLIDAY_SCHEDULE <> S.HOLIDAY_SCHEDULE or 
    T.EXAM_SEAT_SPACING <> S.EXAM_SEAT_SPACING or 
    T.DYN_DT_INCLUDE <> S.DYN_DT_INCLUDE or 
    T.DYN_DT_CALC_REQ <> S.DYN_DT_CALC_REQ or 
    T.ATTEND_GENERATE <> S.ATTEND_GENERATE or 
    T.ATTEND_SYNC_REQD <> S.ATTEND_SYNC_REQD or 
    T.FEES_EXIST <> S.FEES_EXIST or 
    T.CNCL_IF_STUD_ENRLD <> S.CNCL_IF_STUD_ENRLD or 
    T.RCV_FROM_ITEM_TYPE <> S.RCV_FROM_ITEM_TYPE or 
    T.AP_BUS_UNIT <> S.AP_BUS_UNIT or 
    T.AP_LEDGER <> S.AP_LEDGER or 
    T.AP_ACCOUNT <> S.AP_ACCOUNT or 
    T.AP_DEPTID <> S.AP_DEPTID or 
    T.AP_PROJ_ID <> S.AP_PROJ_ID or 
    T.AP_PRODUCT <> S.AP_PRODUCT or 
    T.AP_FUND_CODE <> S.AP_FUND_CODE or 
    T.AP_PROG_CODE <> S.AP_PROG_CODE or 
    T.AP_CLASS_FLD <> S.AP_CLASS_FLD or 
    T.AP_AFFILIATE <> S.AP_AFFILIATE or 
    T.AP_OP_UNIT <> S.AP_OP_UNIT or 
    T.AP_ALTACCT <> S.AP_ALTACCT or 
    T.AP_BUD_REF <> S.AP_BUD_REF or 
    T.AP_CF1 <> S.AP_CF1 or 
    T.AP_CF2 <> S.AP_CF2 or 
    T.AP_CF3 <> S.AP_CF3 or 
    T.AP_AFF_INT1 <> S.AP_AFF_INT1 or 
    T.AP_AFF_INT2 <> S.AP_AFF_INT2 or 
    T.WRITEOFF_BUS_UNIT <> S.WRITEOFF_BUS_UNIT or 
    T.WRITEOFF_LEDGER <> S.WRITEOFF_LEDGER or 
    T.WRITEOFF_ACCOUNT <> S.WRITEOFF_ACCOUNT or 
    T.WRITEOFF_DEPTID <> S.WRITEOFF_DEPTID or 
    T.WRITEOFF_PROJ_ID <> S.WRITEOFF_PROJ_ID or 
    T.WRITEOFF_PRODUCT <> S.WRITEOFF_PRODUCT or 
    T.WRITEOFF_FUND_CODE <> S.WRITEOFF_FUND_CODE or 
    T.WRITEOFF_PROG_CODE <> S.WRITEOFF_PROG_CODE or 
    T.WRITEOFF_CLASS_FLD <> S.WRITEOFF_CLASS_FLD or 
    T.WRITEOFF_AFFILIATE <> S.WRITEOFF_AFFILIATE or 
    T.WRITEOFF_OP_UNIT <> S.WRITEOFF_OP_UNIT or 
    T.WRITEOFF_ALTACCT <> S.WRITEOFF_ALTACCT or 
    T.WRITEOFF_BUD_REF <> S.WRITEOFF_BUD_REF or 
    T.WRITEOFF_CF1 <> S.WRITEOFF_CF1 or 
    T.WRITEOFF_CF2 <> S.WRITEOFF_CF2 or 
    T.WRITEOFF_CF3 <> S.WRITEOFF_CF3 or 
    T.WRITEOFF_AFF_INT1 <> S.WRITEOFF_AFF_INT1 or 
    T.WRITEOFF_AFF_INT2 <> S.WRITEOFF_AFF_INT2 or 
    T.EXT_WRITEOFF <> S.EXT_WRITEOFF or 
    T.GL_INTERFACE_REQ <> S.GL_INTERFACE_REQ or 
    T.LMS_FILE_TYPE <> S.LMS_FILE_TYPE or 
    T.LMS_GROUP_ID <> S.LMS_GROUP_ID or 
    T.LMS_URL <> S.LMS_URL or 
    nvl(trim(T.LMS_CLASS_EXT_DTTM),0) <> nvl(trim(S.LMS_CLASS_EXT_DTTM),0) or 
    nvl(trim(T.LMS_ENRL_EXT_DTTM),0) <> nvl(trim(S.LMS_ENRL_EXT_DTTM),0) or 
    T.LMS_PROVIDER <> S.LMS_PROVIDER or 
    T.SSR_DROP_CONSENT <> S.SSR_DROP_CONSENT or
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.CRSE_ID,
    T.CRSE_OFFER_NBR, 
    T.STRM, 
    T.SESSION_CODE, 
    T.CLASS_SECTION,
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.ACAD_GROUP, 
    T.SUBJECT,
    T.CATALOG_NBR,
    T.ACAD_CAREER,
    T.DESCR,
    T.CLASS_NBR,
    T.SSR_COMPONENT,
    T.ENRL_STAT,
    T.CLASS_STAT, 
    T.CLASS_TYPE, 
    T.ASSOCIATED_CLASS, 
    T.WAITLIST_DAEMON,
    T.AUTO_ENRL_WAITLIST, 
    T.STDNT_SPEC_PERM,
    T.AUTO_ENROLL_SECT_1, 
    T.AUTO_ENROLL_SECT_2, 
    T.RESECTION,
    T.SCHEDULE_PRINT, 
    T.CONSENT,
    T.ENRL_CAP, 
    T.WAIT_CAP, 
    T.MIN_ENRL, 
    T.ENRL_TOT, 
    T.WAIT_TOT, 
    T.CRS_TOPIC_ID, 
    T.PRINT_TOPIC,
    T.ACAD_ORG, 
    T.NEXT_STDNT_POSITIN, 
    T.EMPLID, 
    T.CAMPUS, 
    T.LOCATION, 
    T.CAMPUS_EVENT_NBR, 
    T.INSTRUCTION_MODE, 
    T.EQUIV_CRSE_ID,
    T.OVRD_CRSE_EQUIV_ID, 
    T.ROOM_CAP_REQUEST, 
    T.START_DT, 
    T.END_DT, 
    T.CANCEL_DT,
    T.PRIM_INSTR_SECT,
    T.COMBINED_SECTION, 
    T.HOLIDAY_SCHEDULE, 
    T.EXAM_SEAT_SPACING,
    T.DYN_DT_INCLUDE, 
    T.DYN_DT_CALC_REQ,
    T.ATTEND_GENERATE,
    T.ATTEND_SYNC_REQD, 
    T.FEES_EXIST, 
    T.CNCL_IF_STUD_ENRLD, 
    T.RCV_FROM_ITEM_TYPE, 
    T.AP_BUS_UNIT,
    T.AP_LEDGER,
    T.AP_ACCOUNT, 
    T.AP_DEPTID,
    T.AP_PROJ_ID, 
    T.AP_PRODUCT, 
    T.AP_FUND_CODE, 
    T.AP_PROG_CODE, 
    T.AP_CLASS_FLD, 
    T.AP_AFFILIATE, 
    T.AP_OP_UNIT, 
    T.AP_ALTACCT, 
    T.AP_BUD_REF, 
    T.AP_CF1, 
    T.AP_CF2, 
    T.AP_CF3, 
    T.AP_AFF_INT1,
    T.AP_AFF_INT2,
    T.WRITEOFF_BUS_UNIT,
    T.WRITEOFF_LEDGER,
    T.WRITEOFF_ACCOUNT, 
    T.WRITEOFF_DEPTID,
    T.WRITEOFF_PROJ_ID, 
    T.WRITEOFF_PRODUCT, 
    T.WRITEOFF_FUND_CODE, 
    T.WRITEOFF_PROG_CODE, 
    T.WRITEOFF_CLASS_FLD, 
    T.WRITEOFF_AFFILIATE, 
    T.WRITEOFF_OP_UNIT, 
    T.WRITEOFF_ALTACCT, 
    T.WRITEOFF_BUD_REF, 
    T.WRITEOFF_CF1, 
    T.WRITEOFF_CF2, 
    T.WRITEOFF_CF3, 
    T.WRITEOFF_AFF_INT1,
    T.WRITEOFF_AFF_INT2,
    T.EXT_WRITEOFF, 
    T.GL_INTERFACE_REQ, 
    T.LMS_FILE_TYPE,
    T.LMS_GROUP_ID, 
    T.LMS_URL,
    T.LMS_CLASS_EXT_DTTM, 
    T.LMS_ENRL_EXT_DTTM,
    T.LMS_PROVIDER, 
    T.SSR_DROP_CONSENT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.CRSE_ID,
    S.CRSE_OFFER_NBR, 
    S.STRM, 
    S.SESSION_CODE, 
    S.CLASS_SECTION,
    'CS90', 
    S.INSTITUTION,
    S.ACAD_GROUP, 
    S.SUBJECT,
    S.CATALOG_NBR,
    S.ACAD_CAREER,
    S.DESCR,
    S.CLASS_NBR,
    S.SSR_COMPONENT,
    S.ENRL_STAT,
    S.CLASS_STAT, 
    S.CLASS_TYPE, 
    S.ASSOCIATED_CLASS, 
    S.WAITLIST_DAEMON,
    S.AUTO_ENRL_WAITLIST, 
    S.STDNT_SPEC_PERM,
    S.AUTO_ENROLL_SECT_1, 
    S.AUTO_ENROLL_SECT_2, 
    S.RESECTION,
    S.SCHEDULE_PRINT, 
    S.CONSENT,
    S.ENRL_CAP, 
    S.WAIT_CAP, 
    S.MIN_ENRL, 
    S.ENRL_TOT, 
    S.WAIT_TOT, 
    S.CRS_TOPIC_ID, 
    S.PRINT_TOPIC,
    S.ACAD_ORG, 
    S.NEXT_STDNT_POSITIN, 
    S.EMPLID, 
    S.CAMPUS, 
    S.LOCATION, 
    S.CAMPUS_EVENT_NBR, 
    S.INSTRUCTION_MODE, 
    S.EQUIV_CRSE_ID,
    S.OVRD_CRSE_EQUIV_ID, 
    S.ROOM_CAP_REQUEST, 
    S.START_DT, 
    S.END_DT, 
    S.CANCEL_DT,
    S.PRIM_INSTR_SECT,
    S.COMBINED_SECTION, 
    S.HOLIDAY_SCHEDULE, 
    S.EXAM_SEAT_SPACING,
    S.DYN_DT_INCLUDE, 
    S.DYN_DT_CALC_REQ,
    S.ATTEND_GENERATE,
    S.ATTEND_SYNC_REQD, 
    S.FEES_EXIST, 
    S.CNCL_IF_STUD_ENRLD, 
    S.RCV_FROM_ITEM_TYPE, 
    S.AP_BUS_UNIT,
    S.AP_LEDGER,
    S.AP_ACCOUNT, 
    S.AP_DEPTID,
    S.AP_PROJ_ID, 
    S.AP_PRODUCT, 
    S.AP_FUND_CODE, 
    S.AP_PROG_CODE, 
    S.AP_CLASS_FLD, 
    S.AP_AFFILIATE, 
    S.AP_OP_UNIT, 
    S.AP_ALTACCT, 
    S.AP_BUD_REF, 
    S.AP_CF1, 
    S.AP_CF2, 
    S.AP_CF3, 
    S.AP_AFF_INT1,
    S.AP_AFF_INT2,
    S.WRITEOFF_BUS_UNIT,
    S.WRITEOFF_LEDGER,
    S.WRITEOFF_ACCOUNT, 
    S.WRITEOFF_DEPTID,
    S.WRITEOFF_PROJ_ID, 
    S.WRITEOFF_PRODUCT, 
    S.WRITEOFF_FUND_CODE, 
    S.WRITEOFF_PROG_CODE, 
    S.WRITEOFF_CLASS_FLD, 
    S.WRITEOFF_AFFILIATE, 
    S.WRITEOFF_OP_UNIT, 
    S.WRITEOFF_ALTACCT, 
    S.WRITEOFF_BUD_REF, 
    S.WRITEOFF_CF1, 
    S.WRITEOFF_CF2, 
    S.WRITEOFF_CF3, 
    S.WRITEOFF_AFF_INT1,
    S.WRITEOFF_AFF_INT2,
    S.EXT_WRITEOFF, 
    S.GL_INTERFACE_REQ, 
    S.LMS_FILE_TYPE,
    S.LMS_GROUP_ID, 
    S.LMS_URL,
    S.LMS_CLASS_EXT_DTTM, 
    S.LMS_ENRL_EXT_DTTM,
    S.LMS_PROVIDER, 
    S.SSR_DROP_CONSENT,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_TBL';
update CSSTG_OWNER.PS_CLASS_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION
   from CSSTG_OWNER.PS_CLASS_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_TBL') = 'Y'
  minus
 select nvl(trim(CRSE_ID),'-') CRSE_ID, 
    nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR, 
    nvl(trim(STRM),'-') STRM, 
    nvl(trim(SESSION_CODE),'-') SESSION_CODE, 
    nvl(trim(CLASS_SECTION),'-') CLASS_SECTION
   from SYSADM.PS_CLASS_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_TBL') = 'Y'
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.CLASS_SECTION = S.CLASS_SECTION
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_TBL'
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

END PS_CLASS_TBL_P;
/

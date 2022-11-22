DROP PROCEDURE CSMRT_OWNER.BB_COURSE_USERS_S2_P
/

--
-- BB_COURSE_USERS_S2_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."BB_COURSE_USERS_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_COURSE_USERS_S2 from S1 table BB_COURSE_USERS_S1.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_COURSE_USERS_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_COURSE_USERS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_COURSE_USERS_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_COURSE_USERS_S2 T
using (select /*+ full(S) */
        BB_SOURCE,
		PK1,
        DISPLAY_ORDER, LINK_NAME_3, CRSMAIN_PK1, DATA_SRC_PK1, ROLE, LINK_NAME_1, LINK_URL_1, LINK_DESC_1, LINK_NAME_2, LINK_URL_2, LINK_DESC_2, USERS_PK1, LINK_URL_3, LINK_DESC_3, PHOTO_LINK, INTRO, NOTE, PINFO, USER_HAS_HIDDEN_IND, CARTRIDGE_IND, AVAILABLE_IND, RECEIVE_EMAIL_IND, ROSTER_IND, LIMITED_GRADER_IND, SOS_ID_PK2, ROW_STATUS, ENROLLMENT_DATE, LAST_ACCESS_DATE, CRSMAIN_SOS_ID_PK2, USERS_SOS_ID_PK2, DTMODIFIED, CHILD_CRSMAIN_PK1, ROW_STATUS_CRS_DISABLE, ROW_STATUS_CACHE, COLOR_INDEX, DUE_DATE_EXCEPTION, TIME_LIMIT_EXCEPTION, BYPASS_COURSE_AVAIL_UNTIL, DATE_LAST_MODIFIED, OWNER, FOUNDATIONS_ID, VERSION, 
        DELETE_FLAG, INSERT_TIME, UPDATE_TIME
       from CSSTG_OWNER.BB_COURSE_USERS_S1) S
 on (
    T.BB_SOURCE = S.BB_SOURCE and
    T.PK1 = S.PK1)
 when matched then update set
       T.DISPLAY_ORDER = S.DISPLAY_ORDER,
       T.LINK_NAME_3 = S.LINK_NAME_3,
       T.CRSMAIN_PK1 = S.CRSMAIN_PK1,
       T.DATA_SRC_PK1 = S.DATA_SRC_PK1,
       T.ROLE = S.ROLE,
       T.LINK_NAME_1 = S.LINK_NAME_1,
       T.LINK_URL_1 = S.LINK_URL_1,
       T.LINK_DESC_1 = S.LINK_DESC_1,
       T.LINK_NAME_2 = S.LINK_NAME_2,
       T.LINK_URL_2 = S.LINK_URL_2,
       T.LINK_DESC_2 = S.LINK_DESC_2,
       T.USERS_PK1 = S.USERS_PK1,
       T.LINK_URL_3 = S.LINK_URL_3,
       T.LINK_DESC_3 = S.LINK_DESC_3,
       T.PHOTO_LINK = S.PHOTO_LINK,
       T.INTRO = S.INTRO,
       T.NOTE = S.NOTE,
       T.PINFO = S.PINFO,
       T.USER_HAS_HIDDEN_IND = S.USER_HAS_HIDDEN_IND,
       T.CARTRIDGE_IND = S.CARTRIDGE_IND,
       T.AVAILABLE_IND = S.AVAILABLE_IND,
       T.RECEIVE_EMAIL_IND = S.RECEIVE_EMAIL_IND,
       T.ROSTER_IND = S.ROSTER_IND,
       T.LIMITED_GRADER_IND = S.LIMITED_GRADER_IND,
       T.SOS_ID_PK2 = S.SOS_ID_PK2,
       T.ROW_STATUS = S.ROW_STATUS,
       T.ENROLLMENT_DATE = S.ENROLLMENT_DATE,
       T.LAST_ACCESS_DATE = S.LAST_ACCESS_DATE,
       T.CRSMAIN_SOS_ID_PK2 = S.CRSMAIN_SOS_ID_PK2,
       T.USERS_SOS_ID_PK2 = S.USERS_SOS_ID_PK2,
       T.DTMODIFIED = S.DTMODIFIED,
       T.CHILD_CRSMAIN_PK1 = S.CHILD_CRSMAIN_PK1,
       T.ROW_STATUS_CRS_DISABLE = S.ROW_STATUS_CRS_DISABLE,
       T.ROW_STATUS_CACHE = S.ROW_STATUS_CACHE,
       T.COLOR_INDEX = S.COLOR_INDEX,
       T.DUE_DATE_EXCEPTION = S.DUE_DATE_EXCEPTION,
       T.TIME_LIMIT_EXCEPTION = S.TIME_LIMIT_EXCEPTION,
       T.BYPASS_COURSE_AVAIL_UNTIL = S.BYPASS_COURSE_AVAIL_UNTIL,
       T.DATE_LAST_MODIFIED = S.DATE_LAST_MODIFIED,
       T.OWNER = S.OWNER,
       T.FOUNDATIONS_ID = S.FOUNDATIONS_ID,
       T.VERSION = S.VERSION,
       T.DELETE_FLAG = 'N',
       T.UPDATE_TIME = SYSDATE
where
       decode(T.DISPLAY_ORDER,S.DISPLAY_ORDER,0,1) = 1 or
       decode(T.LINK_NAME_3,S.LINK_NAME_3,0,1) = 1 or
       decode(T.CRSMAIN_PK1,S.CRSMAIN_PK1,0,1) = 1 or
       decode(T.DATA_SRC_PK1,S.DATA_SRC_PK1,0,1) = 1 or
       decode(T.ROLE,S.ROLE,0,1) = 1 or
       decode(T.LINK_NAME_1,S.LINK_NAME_1,0,1) = 1 or
       decode(T.LINK_URL_1,S.LINK_URL_1,0,1) = 1 or
       decode(T.LINK_DESC_1,S.LINK_DESC_1,0,1) = 1 or
       decode(T.LINK_NAME_2,S.LINK_NAME_2,0,1) = 1 or
       decode(T.LINK_URL_2,S.LINK_URL_2,0,1) = 1 or
       decode(T.LINK_DESC_2,S.LINK_DESC_2,0,1) = 1 or
       decode(T.USERS_PK1,S.USERS_PK1,0,1) = 1 or
       decode(T.LINK_URL_3,S.LINK_URL_3,0,1) = 1 or
       decode(T.LINK_DESC_3,S.LINK_DESC_3,0,1) = 1 or
       decode(T.PHOTO_LINK,S.PHOTO_LINK,0,1) = 1 or
       decode(T.INTRO,S.INTRO,0,1) = 1 or
       decode(T.NOTE,S.NOTE,0,1) = 1 or
       decode(T.PINFO,S.PINFO,0,1) = 1 or
       decode(T.USER_HAS_HIDDEN_IND,S.USER_HAS_HIDDEN_IND,0,1) = 1 or
       decode(T.CARTRIDGE_IND,S.CARTRIDGE_IND,0,1) = 1 or
       decode(T.AVAILABLE_IND,S.AVAILABLE_IND,0,1) = 1 or
       decode(T.RECEIVE_EMAIL_IND,S.RECEIVE_EMAIL_IND,0,1) = 1 or
       decode(T.ROSTER_IND,S.ROSTER_IND,0,1) = 1 or
       decode(T.LIMITED_GRADER_IND,S.LIMITED_GRADER_IND,0,1) = 1 or
       decode(T.SOS_ID_PK2,S.SOS_ID_PK2,0,1) = 1 or
       decode(T.ROW_STATUS,S.ROW_STATUS,0,1) = 1 or
       decode(T.ENROLLMENT_DATE,S.ENROLLMENT_DATE,0,1) = 1 or
       decode(T.LAST_ACCESS_DATE,S.LAST_ACCESS_DATE,0,1) = 1 or
       decode(T.CRSMAIN_SOS_ID_PK2,S.CRSMAIN_SOS_ID_PK2,0,1) = 1 or
       decode(T.USERS_SOS_ID_PK2,S.USERS_SOS_ID_PK2,0,1) = 1 or
       decode(T.DTMODIFIED,S.DTMODIFIED,0,1) = 1 or
       decode(T.CHILD_CRSMAIN_PK1,S.CHILD_CRSMAIN_PK1,0,1) = 1 or
       decode(T.ROW_STATUS_CRS_DISABLE,S.ROW_STATUS_CRS_DISABLE,0,1) = 1 or
       decode(T.ROW_STATUS_CACHE,S.ROW_STATUS_CACHE,0,1) = 1 or
       decode(T.COLOR_INDEX,S.COLOR_INDEX,0,1) = 1 or
       decode(T.DUE_DATE_EXCEPTION,S.DUE_DATE_EXCEPTION,0,1) = 1 or
       decode(T.TIME_LIMIT_EXCEPTION,S.TIME_LIMIT_EXCEPTION,0,1) = 1 or
       decode(T.BYPASS_COURSE_AVAIL_UNTIL,S.BYPASS_COURSE_AVAIL_UNTIL,0,1) = 1 or
       decode(T.DATE_LAST_MODIFIED,S.DATE_LAST_MODIFIED,0,1) = 1 or
       decode(T.OWNER,S.OWNER,0,1) = 1 or
       decode(T.FOUNDATIONS_ID,S.FOUNDATIONS_ID,0,1) = 1 or
       decode(T.VERSION,S.VERSION,0,1) = 1 
when not matched then
insert (
       T.BB_SOURCE,
       T.PK1,
       T.DISPLAY_ORDER,
       T.LINK_NAME_3,
       T.CRSMAIN_PK1,
       T.DATA_SRC_PK1,
       T.ROLE,
       T.LINK_NAME_1,
       T.LINK_URL_1,
       T.LINK_DESC_1,
       T.LINK_NAME_2,
       T.LINK_URL_2,
       T.LINK_DESC_2,
       T.USERS_PK1,
       T.LINK_URL_3,
       T.LINK_DESC_3,
       T.PHOTO_LINK,
       T.INTRO,
       T.NOTE,
       T.PINFO,
       T.USER_HAS_HIDDEN_IND,
       T.CARTRIDGE_IND,
       T.AVAILABLE_IND,
       T.RECEIVE_EMAIL_IND,
       T.ROSTER_IND,
       T.LIMITED_GRADER_IND,
       T.SOS_ID_PK2,
       T.ROW_STATUS,
       T.ENROLLMENT_DATE,
       T.LAST_ACCESS_DATE,
       T.CRSMAIN_SOS_ID_PK2,
       T.USERS_SOS_ID_PK2,
       T.DTMODIFIED,
       T.CHILD_CRSMAIN_PK1,
       T.ROW_STATUS_CRS_DISABLE,
       T.ROW_STATUS_CACHE,
       T.COLOR_INDEX,
       T.DUE_DATE_EXCEPTION,
       T.TIME_LIMIT_EXCEPTION,
       T.BYPASS_COURSE_AVAIL_UNTIL,
       T.DATE_LAST_MODIFIED,
       T.OWNER,
       T.FOUNDATIONS_ID,
       T.VERSION,
       T.DELETE_FLAG,
       T.INSERT_TIME,
       T.UPDATE_TIME
)
values (
       S.BB_SOURCE,
       S.PK1,
       S.DISPLAY_ORDER,
       S.LINK_NAME_3,
       S.CRSMAIN_PK1,
       S.DATA_SRC_PK1,
       S.ROLE,
       S.LINK_NAME_1,
       S.LINK_URL_1,
       S.LINK_DESC_1,
       S.LINK_NAME_2,
       S.LINK_URL_2,
       S.LINK_DESC_2,
       S.USERS_PK1,
       S.LINK_URL_3,
       S.LINK_DESC_3,
       S.PHOTO_LINK,
       S.INTRO,
       S.NOTE,
       S.PINFO,
       S.USER_HAS_HIDDEN_IND,
       S.CARTRIDGE_IND,
       S.AVAILABLE_IND,
       S.RECEIVE_EMAIL_IND,
       S.ROSTER_IND,
       S.LIMITED_GRADER_IND,
       S.SOS_ID_PK2,
       S.ROW_STATUS,
       S.ENROLLMENT_DATE,
       S.LAST_ACCESS_DATE,
       S.CRSMAIN_SOS_ID_PK2,
       S.USERS_SOS_ID_PK2,
       S.DTMODIFIED,
       S.CHILD_CRSMAIN_PK1,
       S.ROW_STATUS_CRS_DISABLE,
       S.ROW_STATUS_CACHE,
       S.COLOR_INDEX,
       S.DUE_DATE_EXCEPTION,
       S.TIME_LIMIT_EXCEPTION,
       S.BYPASS_COURSE_AVAIL_UNTIL,
       S.DATE_LAST_MODIFIED,
       S.OWNER,
       S.FOUNDATIONS_ID,
       S.VERSION,
       'N',
       SYSDATE,
       SYSDATE)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_COURSE_USERS_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_COURSE_USERS_S2',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

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

END BB_COURSE_USERS_S2_P;
/

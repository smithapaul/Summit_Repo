DROP PROCEDURE CSMRT_OWNER.BB_GRADEBOOK_GRADE_S2_P
/

--
-- BB_GRADEBOOK_GRADE_S2_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."BB_GRADEBOOK_GRADE_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_GRADEBOOK_GRADE_S2 from S1 table BB_GRADEBOOK_GRADE_S1.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_GRADEBOOK_GRADE_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_GRADEBOOK_GRADE_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_GRADEBOOK_GRADE_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_GRADEBOOK_GRADE_S2 T
using (select /*+ full(S) */
        BB_SOURCE,
		PK1,
        GRADEBOOK_MAIN_PK1, COURSE_USERS_PK1, BATCH_UID, COMMENTS, NOTES_FORMAT_TYPE, FOR_STUDENT_COMMENTS, FEEDBACK_FORMAT_TYPE, DATE_ADDED, DATE_MODIFIED, LAST_OVERRIDE_DATE, LAST_ATTEMPT_DATE, STATUS, HIGHEST_ATTEMPT_PK1, UP_HIGHEST_ATTEMPT_PK1, LOWEST_ATTEMPT_PK1, UP_LOWEST_ATTEMPT_PK1, LAST_ATTEMPT_PK1, FIRST_ATTEMPT_PK1, LAST_GRADED_ATTEMPT_PK1, UP_LAST_GRADED_ATTEMPT_PK1, FIRST_GRADED_ATTEMPT_PK1, UP_FIRST_GRADED_ATTEMPT_PK1, EXEMPT_ATTEMPT_IND, AVERAGE_SCORE, UP_AVERAGE_SCORE, MANUAL_GRADE, MANUAL_SCORE, VERSION, EXEMPT_IND, EXCLUDED_IND, PENDING_MANUAL_GRADE, PENDING_MANUAL_SCORE, PERF_CODE_PK1, AUTOMATIC_ZERO, 
        DELETE_FLAG, INSERT_TIME, UPDATE_TIME
       from CSSTG_OWNER.BB_GRADEBOOK_GRADE_S1) S
 on (
    T.BB_SOURCE = S.BB_SOURCE and
    T.PK1 = S.PK1)
 when matched then update set
       T.GRADEBOOK_MAIN_PK1 = S.GRADEBOOK_MAIN_PK1,
       T.COURSE_USERS_PK1 = S.COURSE_USERS_PK1,
       T.BATCH_UID = S.BATCH_UID,
       T.COMMENTS = S.COMMENTS,
       T.NOTES_FORMAT_TYPE = S.NOTES_FORMAT_TYPE,
       T.FOR_STUDENT_COMMENTS = S.FOR_STUDENT_COMMENTS,
       T.FEEDBACK_FORMAT_TYPE = S.FEEDBACK_FORMAT_TYPE,
       T.DATE_ADDED = S.DATE_ADDED,
       T.DATE_MODIFIED = S.DATE_MODIFIED,
       T.LAST_OVERRIDE_DATE = S.LAST_OVERRIDE_DATE,
       T.LAST_ATTEMPT_DATE = S.LAST_ATTEMPT_DATE,
       T.STATUS = S.STATUS,
       T.HIGHEST_ATTEMPT_PK1 = S.HIGHEST_ATTEMPT_PK1,
       T.UP_HIGHEST_ATTEMPT_PK1 = S.UP_HIGHEST_ATTEMPT_PK1,
       T.LOWEST_ATTEMPT_PK1 = S.LOWEST_ATTEMPT_PK1,
       T.UP_LOWEST_ATTEMPT_PK1 = S.UP_LOWEST_ATTEMPT_PK1,
       T.LAST_ATTEMPT_PK1 = S.LAST_ATTEMPT_PK1,
       T.FIRST_ATTEMPT_PK1 = S.FIRST_ATTEMPT_PK1,
       T.LAST_GRADED_ATTEMPT_PK1 = S.LAST_GRADED_ATTEMPT_PK1,
       T.UP_LAST_GRADED_ATTEMPT_PK1 = S.UP_LAST_GRADED_ATTEMPT_PK1,
       T.FIRST_GRADED_ATTEMPT_PK1 = S.FIRST_GRADED_ATTEMPT_PK1,
       T.UP_FIRST_GRADED_ATTEMPT_PK1 = S.UP_FIRST_GRADED_ATTEMPT_PK1,
       T.EXEMPT_ATTEMPT_IND = S.EXEMPT_ATTEMPT_IND,
       T.AVERAGE_SCORE = S.AVERAGE_SCORE,
       T.UP_AVERAGE_SCORE = S.UP_AVERAGE_SCORE,
       T.MANUAL_GRADE = S.MANUAL_GRADE,
       T.MANUAL_SCORE = S.MANUAL_SCORE,
       T.VERSION = S.VERSION,
       T.EXEMPT_IND = S.EXEMPT_IND,
       T.EXCLUDED_IND = S.EXCLUDED_IND,
       T.PENDING_MANUAL_GRADE = S.PENDING_MANUAL_GRADE,
       T.PENDING_MANUAL_SCORE = S.PENDING_MANUAL_SCORE,
       T.PERF_CODE_PK1 = S.PERF_CODE_PK1,
       T.AUTOMATIC_ZERO = S.AUTOMATIC_ZERO,
       T.DELETE_FLAG = 'N',
       T.UPDATE_TIME = SYSDATE
where
       decode(T.GRADEBOOK_MAIN_PK1,S.GRADEBOOK_MAIN_PK1,0,1) = 1 or
       decode(T.COURSE_USERS_PK1,S.COURSE_USERS_PK1,0,1) = 1 or
       decode(T.BATCH_UID,S.BATCH_UID,0,1) = 1 or
       decode(T.COMMENTS,S.COMMENTS,0,1) = 1 or
       decode(T.NOTES_FORMAT_TYPE,S.NOTES_FORMAT_TYPE,0,1) = 1 or
       decode(T.FOR_STUDENT_COMMENTS,S.FOR_STUDENT_COMMENTS,0,1) = 1 or
       decode(T.FEEDBACK_FORMAT_TYPE,S.FEEDBACK_FORMAT_TYPE,0,1) = 1 or
       decode(T.DATE_ADDED,S.DATE_ADDED,0,1) = 1 or
       decode(T.DATE_MODIFIED,S.DATE_MODIFIED,0,1) = 1 or
       decode(T.LAST_OVERRIDE_DATE,S.LAST_OVERRIDE_DATE,0,1) = 1 or
       decode(T.LAST_ATTEMPT_DATE,S.LAST_ATTEMPT_DATE,0,1) = 1 or
       decode(T.STATUS,S.STATUS,0,1) = 1 or
       decode(T.HIGHEST_ATTEMPT_PK1,S.HIGHEST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.UP_HIGHEST_ATTEMPT_PK1,S.UP_HIGHEST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.LOWEST_ATTEMPT_PK1,S.LOWEST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.UP_LOWEST_ATTEMPT_PK1,S.UP_LOWEST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.LAST_ATTEMPT_PK1,S.LAST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.FIRST_ATTEMPT_PK1,S.FIRST_ATTEMPT_PK1,0,1) = 1 or
       decode(T.LAST_GRADED_ATTEMPT_PK1,S.LAST_GRADED_ATTEMPT_PK1,0,1) = 1 or
       decode(T.UP_LAST_GRADED_ATTEMPT_PK1,S.UP_LAST_GRADED_ATTEMPT_PK1,0,1) = 1 or
       decode(T.FIRST_GRADED_ATTEMPT_PK1,S.FIRST_GRADED_ATTEMPT_PK1,0,1) = 1 or
       decode(T.UP_FIRST_GRADED_ATTEMPT_PK1,S.UP_FIRST_GRADED_ATTEMPT_PK1,0,1) = 1 or
       decode(T.EXEMPT_ATTEMPT_IND,S.EXEMPT_ATTEMPT_IND,0,1) = 1 or
       decode(T.AVERAGE_SCORE,S.AVERAGE_SCORE,0,1) = 1 or
       decode(T.UP_AVERAGE_SCORE,S.UP_AVERAGE_SCORE,0,1) = 1 or
       decode(T.MANUAL_GRADE,S.MANUAL_GRADE,0,1) = 1 or
       decode(T.MANUAL_SCORE,S.MANUAL_SCORE,0,1) = 1 or
       decode(T.VERSION,S.VERSION,0,1) = 1 or
       decode(T.EXEMPT_IND,S.EXEMPT_IND,0,1) = 1 or
       decode(T.EXCLUDED_IND,S.EXCLUDED_IND,0,1) = 1 or
       decode(T.PENDING_MANUAL_GRADE,S.PENDING_MANUAL_GRADE,0,1) = 1 or
       decode(T.PENDING_MANUAL_SCORE,S.PENDING_MANUAL_SCORE,0,1) = 1 or
       decode(T.PERF_CODE_PK1,S.PERF_CODE_PK1,0,1) = 1 or
       decode(T.AUTOMATIC_ZERO,S.AUTOMATIC_ZERO,0,1) = 1 
when not matched then
insert (
       T.BB_SOURCE,
       T.PK1,
       T.GRADEBOOK_MAIN_PK1,
       T.COURSE_USERS_PK1,
       T.BATCH_UID,
       T.COMMENTS,
       T.NOTES_FORMAT_TYPE,
       T.FOR_STUDENT_COMMENTS,
       T.FEEDBACK_FORMAT_TYPE,
       T.DATE_ADDED,
       T.DATE_MODIFIED,
       T.LAST_OVERRIDE_DATE,
       T.LAST_ATTEMPT_DATE,
       T.STATUS,
       T.HIGHEST_ATTEMPT_PK1,
       T.UP_HIGHEST_ATTEMPT_PK1,
       T.LOWEST_ATTEMPT_PK1,
       T.UP_LOWEST_ATTEMPT_PK1,
       T.LAST_ATTEMPT_PK1,
       T.FIRST_ATTEMPT_PK1,
       T.LAST_GRADED_ATTEMPT_PK1,
       T.UP_LAST_GRADED_ATTEMPT_PK1,
       T.FIRST_GRADED_ATTEMPT_PK1,
       T.UP_FIRST_GRADED_ATTEMPT_PK1,
       T.EXEMPT_ATTEMPT_IND,
       T.AVERAGE_SCORE,
       T.UP_AVERAGE_SCORE,
       T.MANUAL_GRADE,
       T.MANUAL_SCORE,
       T.VERSION,
       T.EXEMPT_IND,
       T.EXCLUDED_IND,
       T.PENDING_MANUAL_GRADE,
       T.PENDING_MANUAL_SCORE,
       T.PERF_CODE_PK1,
       T.AUTOMATIC_ZERO,
       T.DELETE_FLAG,
       T.INSERT_TIME,
       T.UPDATE_TIME
)
values (
       S.BB_SOURCE,
       S.PK1,
       S.GRADEBOOK_MAIN_PK1,
       S.COURSE_USERS_PK1,
       S.BATCH_UID,
       S.COMMENTS,
       S.NOTES_FORMAT_TYPE,
       S.FOR_STUDENT_COMMENTS,
       S.FEEDBACK_FORMAT_TYPE,
       S.DATE_ADDED,
       S.DATE_MODIFIED,
       S.LAST_OVERRIDE_DATE,
       S.LAST_ATTEMPT_DATE,
       S.STATUS,
       S.HIGHEST_ATTEMPT_PK1,
       S.UP_HIGHEST_ATTEMPT_PK1,
       S.LOWEST_ATTEMPT_PK1,
       S.UP_LOWEST_ATTEMPT_PK1,
       S.LAST_ATTEMPT_PK1,
       S.FIRST_ATTEMPT_PK1,
       S.LAST_GRADED_ATTEMPT_PK1,
       S.UP_LAST_GRADED_ATTEMPT_PK1,
       S.FIRST_GRADED_ATTEMPT_PK1,
       S.UP_FIRST_GRADED_ATTEMPT_PK1,
       S.EXEMPT_ATTEMPT_IND,
       S.AVERAGE_SCORE,
       S.UP_AVERAGE_SCORE,
       S.MANUAL_GRADE,
       S.MANUAL_SCORE,
       S.VERSION,
       S.EXEMPT_IND,
       S.EXCLUDED_IND,
       S.PENDING_MANUAL_GRADE,
       S.PENDING_MANUAL_SCORE,
       S.PERF_CODE_PK1,
       S.AUTOMATIC_ZERO,
       'N',
       SYSDATE,
       SYSDATE)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_GRADEBOOK_GRADE_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_GRADEBOOK_GRADE_S2',
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

END BB_GRADEBOOK_GRADE_S2_P;
/
